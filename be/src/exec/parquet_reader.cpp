// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include "exec/parquet_reader.h"

#include <arrow/status.h>
#include <arrow/array.h>
#include "exec/file_reader.h"
#include "common/logging.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/thrift_util.h"
#include "runtime/tuple.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"

namespace doris {

// Broker

ParquetReaderWrap::ParquetReaderWrap(FileReader *file_reader) :
           _total_groups(0), _current_group(0), _rows_of_group(0), _current_line_of_group(0) {
    _parquet = std::shared_ptr<ParquetFile>(new ParquetFile(file_reader));
    _properties = parquet::ReaderProperties();
    _properties.enable_buffered_stream();
    _properties.set_buffer_size(65535);
}

ParquetReaderWrap::~ParquetReaderWrap() {
    close();
}

#ifdef BE_TEST
inline BrokerServiceClientCache* client_cache(ExecEnv* env) {
    static BrokerServiceClientCache s_client_cache;
    return &s_client_cache;
}

inline const std::string& client_id(ExecEnv* env, const TNetworkAddress& addr) {
    static std::string s_client_id = "doris_unit_test";
    return s_client_id;
}
#else
inline BrokerServiceClientCache* client_cache(ExecEnv* env) {
    return env->broker_client_cache();
}

inline const std::string& client_id(ExecEnv* env, const TNetworkAddress& addr) {
    return env->broker_mgr()->get_client_id(addr);
}
#endif

void ParquetReaderWrap::inital_parquet_reader() {
    //new file reader for parquet file
    _reader.reset(new parquet::arrow::FileReader(arrow::default_memory_pool(),
            std::move(parquet::ParquetFileReader::Open(_parquet, _properties))));

    _file_metadata = _reader->parquet_reader()->metadata();
    // initial members
    _total_groups = _file_metadata->num_row_groups();
    _rows_of_group = _file_metadata->RowGroup(0)->num_rows();

    // map
    const parquet::SchemaDescriptor* schemaDescriptor = _file_metadata->schema();
    for (int i = 0; i < _file_metadata->num_columns(); ++i) {
        // Get the Column Reader for the boolean column
        _map_column.insert(std::pair<std::string, int>(schemaDescriptor->Column(i)->name(), i));
    }
}

void ParquetReaderWrap::close() {
    _parquet->Close();
}

Status ParquetReaderWrap::size(int64_t* size) {
    _parquet->GetSize(size);
    return Status::OK;
}

void ParquetReaderWrap::fill_solt(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool, const uint8_t* value, int32_t len) {
    if (len != 0) {
        tuple->set_not_null(slot_desc->null_indicator_offset());
        void* slot = tuple->get_slot(slot_desc->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->ptr = reinterpret_cast<char*>(mem_pool->allocate(len));
        memcpy(str_slot->ptr, value, len);
        str_slot->len = len;
    }
    return;
}

Status ParquetReaderWrap::column_indices(const std::vector<SlotDescriptor*>& tuple_slot_descs)
{
    _column_ids.clear();
    for (auto slot_desc : tuple_slot_descs) {
        // Get the Column Reader for the boolean column
        auto iter = _map_column.find(slot_desc->col_name());
        if (iter == _map_column.end()) {
            std::stringstream str_error;
            str_error<<"Invalid Column Name:" << slot_desc->col_name();
            return Status(str_error.str());
        }
        _column_ids.emplace_back(iter->second);
    }
    return Status::OK;
}

void ParquetReaderWrap::set_filed_null(Tuple* tuple, const SlotDescriptor* slot_desc) {
    if (!slot_desc->is_nullable()) {
        throw parquet::ParquetException("Null is not allowed, but Parquet field is NULL.");
    }
    tuple->set_null(slot_desc->null_indicator_offset());
}

Status ParquetReaderWrap::read_record_batch(const std::vector<SlotDescriptor*>& tuple_slot_descs, bool* eof) {
    if (_current_line_of_group == 0) {// the first read
        RETURN_IF_ERROR(column_indices(tuple_slot_descs));
        // read batch
        _reader->GetRecordBatchReader({_current_group}, _column_ids, &_rb_batch);
        arrow::Status status = _rb_batch->ReadNext(&_batch);
        if (!status.ok()) {
            LOG(WARNING) << "The first read record. " << status.ToString();
            throw parquet::ParquetException(status.ToString());
        }
    } else if (_current_line_of_group >= _rows_of_group) {// read next row group
        _current_group++;
        if (_current_group >= _total_groups) {// read completed.
            _column_ids.clear();
            *eof = true;
            return Status::OK;
        }
        _current_line_of_group = 0;
        _rows_of_group = _file_metadata->RowGroup(_current_group)->num_rows(); //get rows of the current row group
        // read batch
        _reader->GetRecordBatchReader({_current_group}, _column_ids, &_rb_batch);
        arrow::Status status = _rb_batch->ReadNext(&_batch);
        if (!status.ok()) {
            LOG(WARNING) << status.ToString();
            throw parquet::ParquetException(status.ToString());
        }
    }
    return Status::OK;
}

Status ParquetReaderWrap::read(Tuple* tuple, const std::vector<SlotDescriptor*>& tuple_slot_descs, MemPool* mem_pool, bool* eof) {
    uint8_t tmp_buf[128] = {0};
    int32_t wbtyes = 0;
    int column_index = 0;
    try {
        Status status = read_record_batch(tuple_slot_descs, eof); //read record batch
        if (*eof) {// read over
            return status;
        }

        std::shared_ptr<arrow::Schema> fieldSchema = _batch->schema();
        for (int i = 0; i < tuple_slot_descs.size(); i++) {
            // Get the Column Reader for the boolean column
            auto slot_desc = tuple_slot_descs[i];
            column_index = _column_ids[i];// column index with Parquet Field

            switch (fieldSchema->field(column_index)->type()->id()) {
                case arrow::Type::type::BINARY: {
                    auto str_array = std::dynamic_pointer_cast<arrow::BinaryArray>(_batch->column(column_index));
                    if (str_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                      int32_t length = 0;
                      const uint8_t *p = str_array->GetValue(_current_line_of_group, &length);
                      fill_solt(tuple, slot_desc, mem_pool, p, length);
                    }
                    break;
                }
                /// Signed 8-bit little-endian integer
                case arrow::Type::type::FIXED_SIZE_BINARY: {
                    auto fixed_array = std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(_batch->column(column_index));
                    if (fixed_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        string value = fixed_array->GetString(_current_line_of_group);
                        fill_solt(tuple, slot_desc, mem_pool, (uint8_t*)value.c_str(), value.length());
                    }
                    break;
                }
                case arrow::Type::type::STRING: {
                    auto str_array = std::dynamic_pointer_cast<arrow::StringArray>(_batch->column(column_index));
                    if (str_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        string value = str_array->GetString(_current_line_of_group);
                        fill_solt(tuple, slot_desc, mem_pool, (uint8_t*)value.c_str(), value.length());
                    }
                    break;
                }
                case arrow::Type::type::UINT64: {
                    auto uint64_array = std::dynamic_pointer_cast<arrow::UInt64Array>(_batch->column(column_index));
                    if (uint64_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        uint64_t value = uint64_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%lu", value);
                        fill_solt(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::INT64: {
                    auto int64_array = std::dynamic_pointer_cast<arrow::Int64Array>(_batch->column(column_index));
                    if (int64_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        int64_t value = int64_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%ld", value);
                        fill_solt(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::BOOL: {
                    auto boolean_array = std::dynamic_pointer_cast<arrow::BooleanArray>(_batch->column(column_index));
                    if (boolean_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        bool value = boolean_array->Value(_current_line_of_group);
                        if (value) {
                            fill_solt(tuple, slot_desc, mem_pool, (uint8_t*)"true", 4);
                        } else {
                            fill_solt(tuple, slot_desc, mem_pool, (uint8_t*)"false", 5);
                        }
                    }
                    break;
                }
                case arrow::Type::type::UINT8: {
                    auto uint8_array = std::dynamic_pointer_cast<arrow::UInt8Array>(_batch->column(column_index));
                    if (uint8_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        uint8_t value = uint8_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%d", value);
                        fill_solt(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::INT8: {
                    auto int8_array = std::dynamic_pointer_cast<arrow::Int8Array>(_batch->column(column_index));
                    if (int8_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        int8_t value = int8_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%d", value);
                        fill_solt(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::UINT16: {
                    auto uint16_array = std::dynamic_pointer_cast<arrow::UInt16Array>(_batch->column(column_index));
                    if (uint16_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        uint16_t value = uint16_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%d", value);
                        fill_solt(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::INT16: {
                    auto int16_array = std::dynamic_pointer_cast<arrow::Int16Array>(_batch->column(column_index));
                    if (int16_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        int16_t value = int16_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%d", value);
                        fill_solt(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::UINT32: {
                    auto uint32_array = std::dynamic_pointer_cast<arrow::UInt32Array>(_batch->column(column_index));
                    if (uint32_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        uint32_t value = uint32_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%u", value);
                        fill_solt(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::INT32: {
                    auto int32_array = std::dynamic_pointer_cast<arrow::Int32Array>(_batch->column(column_index));
                    if (int32_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        int32_t value = int32_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%d", value);
                        fill_solt(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::HALF_FLOAT: {
                    auto half_float_array = std::dynamic_pointer_cast<arrow::HalfFloatArray>(_batch->column(column_index));
                    if (half_float_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        float value = half_float_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%f", value);
                        fill_solt(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::FLOAT: {
                    auto float_array = std::dynamic_pointer_cast<arrow::FloatArray>(_batch->column(column_index));
                    if (float_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        float value = float_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%f", value);
                        fill_solt(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::DOUBLE: {
                    auto double_array = std::dynamic_pointer_cast<arrow::DoubleArray>(_batch->column(column_index));
                    if (double_array->IsNull(_current_line_of_group)) {
                        set_filed_null(tuple, slot_desc);
                    } else {
                        float value = double_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%f", value);
                        fill_solt(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                default: {
                    // other type not support.
                    std::stringstream str_error;
                    str_error << "The field type("<< fieldSchema->field(column_index)->type()->id() <<
                            ") not support. RowGroup: " << _current_group
                            << ", Row: " << _current_line_of_group << ", ColumnIndex:" << column_index;
                    LOG(WARNING) << str_error.str();
                    return Status(str_error.str());
                }
            }
        }
    } catch (parquet::ParquetException& e) {
        std::stringstream str_error;
        str_error << e.what() << " RowGroup:" << _current_group << ", Row:" << _current_line_of_group
                            << ", ColumnIndex " << column_index;
        LOG(WARNING) << str_error.str();
        return Status(str_error.str());
    }

    // update data value
    _current_line_of_group++;
    *eof = false;
    return Status::OK;
}

ParquetFile::ParquetFile(FileReader *file): _file(file) {

}

ParquetFile::~ParquetFile() {
    Close();
}

arrow::Status ParquetFile::Close() {
    if (_file) {
        _file->close();
        delete _file;
        _file = nullptr;
    }
    return arrow::Status::OK();
}

bool ParquetFile::closed() const {
    if (_file) {
        return _file->closed();
    } else {
        return true;
    }
}

arrow::Status ParquetFile::Read(int64_t nbytes, int64_t* bytes_read, void* buffer) {
    bool eof = false;
    size_t data_size = 0;
    do {
        data_size = nbytes;
        Status result = _file->read((uint8_t*)buffer, &data_size, &eof);
        if (!result.ok()) {
            return arrow::Status::IOError("Read failed.");
        }
        if (eof) {
            break;
        }
        *bytes_read += data_size; // total read bytes
        nbytes -= data_size; // remained bytes
        buffer = (uint8_t*)buffer + data_size;
    } while (nbytes != 0);
    return arrow::Status::OK();
}

arrow::Status ParquetFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    int64_t reads = 0;
    while(nbytes != 0) {
        Status result = _file->readat(position, nbytes, &reads, out);
        if (!result.ok()) {
            *bytes_read = 0;
            return arrow::Status::IOError("Readat failed.");
        }
        if (reads == 0) {
            break;
        }
        *bytes_read += reads;// total read bytes
        nbytes -= reads; // remained bytes
        position += reads;
        out = (char*)out + reads;
    }
    return arrow::Status::OK();
}

arrow::Status ParquetFile::GetSize(int64_t* size) {
    *size = _file->size();
    return arrow::Status::OK();
}

arrow::Status ParquetFile::Seek(int64_t position) {
    _file->seek(position);
    return arrow::Status::OK();
}


arrow::Status ParquetFile::Tell(int64_t* position) const {
    _file->tell(position);
    return arrow::Status::OK();
}

arrow::Status ParquetFile::Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) {
    return arrow::Status::NotImplemented("Not Supported.");
}

}
