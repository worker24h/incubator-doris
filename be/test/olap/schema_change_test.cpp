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

#include <gtest/gtest.h>

#include "olap/byte_buffer.h"
#include "olap/stream_name.h"
#include "olap/rowset/column_reader.h"
#include "olap/rowset/column_writer.h"
#include "olap/field.h"
#include "olap/olap_define.h"
#include "olap/olap_common.h"
#include "olap/row_cursor.h"
#include "olap/row_block.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"
#include "util/logging.h"

using std::string;

namespace doris {

class TestColumn : public testing::Test {
public:
    TestColumn() : 
            _column_writer(NULL),
            _column_reader(NULL),
            _stream_factory(NULL) {
            _offsets.clear();
        _map_in_streams.clear();
        _present_buffers.clear();        
        _data_buffers.clear();
        _second_buffers.clear();
        _dictionary_buffers.clear();
        _length_buffers.clear();
        _mem_tracker.reset(new MemTracker(-1));
        _mem_pool.reset(new MemPool(_mem_tracker.get()));
    }
    
    virtual ~TestColumn() {
        SAFE_DELETE(_column_writer);
        SAFE_DELETE(_column_reader);
        SAFE_DELETE(_stream_factory);
    }
    
    virtual void SetUp() {
        _offsets.push_back(0);
        _stream_factory = 
                new(std::nothrow) OutStreamFactory(COMPRESS_LZ4,
                                                   OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);
        ASSERT_TRUE(_stream_factory != NULL);
        config::column_dictionary_key_ration_threshold = 30;
        config::column_dictionary_key_size_threshold = 1000;
    }
    
    virtual void TearDown() {
        SAFE_DELETE(_column_writer);
        SAFE_DELETE(_column_reader);
        SAFE_DELETE(_stream_factory);
        SAFE_DELETE(_shared_buffer);

        _offsets.clear();
        _map_in_streams.clear();
        _present_buffers.clear();
        _data_buffers.clear();
        _second_buffers.clear();
        _dictionary_buffers.clear();
        _length_buffers.clear();
    }

    void CreateColumnWriter(const TabletSchema& tablet_schema) {
        _column_writer = ColumnWriter::create(
                0, tablet_schema, _stream_factory, 1024, BLOOM_FILTER_DEFAULT_FPP);        
        ASSERT_TRUE(_column_writer != NULL);
        ASSERT_EQ(_column_writer->init(), OLAP_SUCCESS);
    }

    void CreateColumnReader(const TabletSchema& tablet_schema) {
        UniqueIdEncodingMap encodings;
        encodings[0] = ColumnEncodingMessage();
        encodings[0].set_kind(ColumnEncodingMessage::DIRECT);
        encodings[0].set_dictionary_size(1);
        CreateColumnReader(tablet_schema, encodings);
    }

    void CreateColumnReader(
            const TabletSchema& tablet_schema,
            UniqueIdEncodingMap &encodings) {
        UniqueIdToColumnIdMap included;
        included[0] = 0;
        UniqueIdToColumnIdMap segment_included;
        segment_included[0] = 0;

        _column_reader = ColumnReader::create(0,
                                     tablet_schema,
                                     included,
                                     segment_included,
                                     encodings);
        
        ASSERT_TRUE(_column_reader != NULL);

        system("mkdir -p ./ut_dir");
        system("rm ./ut_dir/tmp_file");

        ASSERT_EQ(OLAP_SUCCESS, 
                  helper.open_with_mode("./ut_dir/tmp_file", 
                                        O_CREAT | O_EXCL | O_WRONLY, 
                                        S_IRUSR | S_IWUSR));
        std::vector<int> off;
        std::vector<int> length;
        std::vector<int> buffer_size;
        std::vector<StreamName> name;

        std::map<StreamName, OutStream*>::const_iterator it 
            = _stream_factory->streams().begin();
        for (; it != _stream_factory->streams().end(); ++it) {
            StreamName stream_name = it->first;
            OutStream *out_stream = it->second;
            std::vector<StorageByteBuffer*> *buffers;

            if (out_stream->is_suppressed()) {
                continue;
            }
            if (stream_name.kind() == StreamInfoMessage::ROW_INDEX) {
                continue;
            } else if (stream_name.kind() == StreamInfoMessage::PRESENT) {
                buffers = &_present_buffers;
            } else if (stream_name.kind() == StreamInfoMessage::DATA) {
                buffers = &_data_buffers;
            } else if (stream_name.kind() == StreamInfoMessage::SECONDARY) {
                buffers = &_second_buffers;
            } else if (stream_name.kind() == StreamInfoMessage::DICTIONARY_DATA) {
                buffers = &_dictionary_buffers;
            } else if (stream_name.kind() == StreamInfoMessage::LENGTH) {
                buffers = &_length_buffers;
            } else {
                ASSERT_TRUE(false);
            }
            
            ASSERT_TRUE(buffers != NULL);
            off.push_back(helper.tell());
            out_stream->write_to_file(&helper, 0);
            length.push_back(out_stream->get_stream_length());
            buffer_size.push_back(out_stream->get_total_buffer_size());
            name.push_back(stream_name);
        }
        helper.close();

        ASSERT_EQ(OLAP_SUCCESS, helper.open_with_mode("./ut_dir/tmp_file", 
                O_RDONLY, S_IRUSR | S_IWUSR)); 

        _shared_buffer = StorageByteBuffer::create(
                OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE + sizeof(StreamHead));
        ASSERT_TRUE(_shared_buffer != NULL);

        for (int i = 0; i < off.size(); ++i) {
            ReadOnlyFileStream* in_stream = new (std::nothrow) ReadOnlyFileStream(
                    &helper, 
                    &_shared_buffer,
                    off[i], 
                    length[i], 
                    lz4_decompress, 
                    buffer_size[i],
                    &_stats);
            ASSERT_EQ(OLAP_SUCCESS, in_stream->init());

            _map_in_streams[name[i]] = in_stream;
        }

        ASSERT_EQ(_column_reader->init(
                   &_map_in_streams,
                   1024,
                   _mem_pool.get(),
                   &_stats), OLAP_SUCCESS);
    }

    void SetTabletSchema(const std::string& name,
            const std::string& type,
            const std::string& aggregation,
            uint32_t length,
            bool is_allow_null,
            bool is_key,
            TabletSchema* tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        ColumnPB* column = tablet_schema_pb.add_column();
        column->set_unique_id(0);
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(is_key);
        column->set_is_nullable(is_allow_null);
        column->set_length(length);
        column->set_aggregation(aggregation);
        tablet_schema->init_from_pb(tablet_schema_pb);
    }

    void create_and_save_last_position() {
        ASSERT_EQ(_column_writer->create_row_index_entry(), OLAP_SUCCESS);
    }

    template <typename T>
    void test_convert_to_varchar(const std::string& type_name, int type_size, T val, const std::string& expected_val, OLAPStatus expected_st) {
        TabletSchema src_tablet_schema;
        SetTabletSchema("ConvertColumn", type_name, "REPLACE", type_size, false, false, &src_tablet_schema);
        CreateColumnWriter(src_tablet_schema);

        RowCursor write_row;
        write_row.init(src_tablet_schema);
        RowBlock block(&src_tablet_schema);
        RowBlockInfo block_info;
        block_info.row_num = 10000;
        block.init(block_info);
        write_row.set_field_content(0, reinterpret_cast<char*>(&val), _mem_pool.get());
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
        ColumnDataHeaderMessage header;
        ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);
        helper.close();

        TabletSchema dst_tablet_schema;
        SetTabletSchema("VarcharColumn", "VARCHAR", "REPLACE", 255, false, false, &dst_tablet_schema);
        CreateColumnReader(src_tablet_schema);
        RowCursor read_row;
        read_row.init(dst_tablet_schema);

        _col_vector.reset(new ColumnVector());
        ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
        char* data = reinterpret_cast<char*>(_col_vector->col_data());
        auto st = read_row.convert_from(0, data, write_row.column_schema(0)->type_info(), _mem_pool.get());
        ASSERT_EQ(st, expected_st);
        if (st == OLAP_SUCCESS) {
            std::string dst_str = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
            ASSERT_TRUE(dst_str.compare(0, expected_val.size(), expected_val) == 0);
        }

        TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
        st = read_row.convert_from(0, read_row.cell_ptr(0), tp, _mem_pool.get());
        ASSERT_EQ(st, OLAP_ERR_INVALID_SCHEMA);
    }

    void test_convert_from_varchar(const std::string& type_name, int type_size, const std::string& value, OLAPStatus expected_st) {
        TabletSchema tablet_schema;
        SetTabletSchema("VarcharColumn", "VARCHAR", "REPLACE", 255, false, false, &tablet_schema);
        CreateColumnWriter(tablet_schema);

        RowCursor write_row;
        write_row.init(tablet_schema);
        RowBlock block(&tablet_schema);
        RowBlockInfo block_info;
        block_info.row_num = 10000;
        block.init(block_info);
        Slice normal_str(value);
        write_row.set_field_content(0, reinterpret_cast<char*>(&normal_str), _mem_pool.get());
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
        ColumnDataHeaderMessage header;
        ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);
        helper.close();

        TabletSchema converted_tablet_schema;
        SetTabletSchema("ConvertColumn", type_name, "REPLACE", type_size, false, false, &converted_tablet_schema);
        CreateColumnReader(tablet_schema);
        RowCursor read_row;
        read_row.init(converted_tablet_schema);

        _col_vector.reset(new ColumnVector());
        ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
        char* data = reinterpret_cast<char*>(_col_vector->col_data());
        auto st = read_row.convert_from(0, data, write_row.column_schema(0)->type_info(), _mem_pool.get());
        ASSERT_EQ(st, expected_st);
        if (st == OLAP_SUCCESS) {
            std::string dst_str = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
            ASSERT_TRUE(dst_str.compare(0, value.size(), value) == 0);
        }

        TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
        st = read_row.convert_from(0, read_row.cell_ptr(0), tp, _mem_pool.get());
        ASSERT_EQ(st, OLAP_ERR_INVALID_SCHEMA);
    }

    ColumnWriter *_column_writer;

    ColumnReader *_column_reader;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<ColumnVector> _col_vector;

    OutStreamFactory *_stream_factory;

    std::vector<size_t> _offsets;
    std::vector<StorageByteBuffer*> _present_buffers;
    std::vector<StorageByteBuffer*> _data_buffers;
    std::vector<StorageByteBuffer*> _second_buffers;
    std::vector<StorageByteBuffer*> _dictionary_buffers;
    std::vector<StorageByteBuffer*> _length_buffers;
    StorageByteBuffer* _shared_buffer;
    std::map<StreamName, ReadOnlyFileStream *> _map_in_streams;
    FileHandler helper;
    OlapReaderStatistics _stats;
};


TEST_F(TestColumn, ConvertFloatToDouble) {
    TabletSchema tablet_schema;
    SetTabletSchema("FloatColumn", "FLOAT", "REPLACE", 4, false, false, &tablet_schema);
    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    float value = 1.234;
    write_row.set_field_content(0, reinterpret_cast<char *>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    value = 3.234;
    write_row.set_field_content(0, reinterpret_cast<char *>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    TabletSchema convert_tablet_schema;
    SetTabletSchema("DoubleColumn", "DOUBLE", "REPLACE", 4, false, false, &convert_tablet_schema);
    CreateColumnReader(tablet_schema);
    RowCursor read_row;
    read_row.init(convert_tablet_schema);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(
        _col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.convert_from(0, data, write_row.column_schema(0)->type_info(), _mem_pool.get());
    //float val1 = *reinterpret_cast<float*>(read_row.cell_ptr(0));
    double val2 = *reinterpret_cast<double*>(read_row.cell_ptr(0));
    
    char buf[64];
    memset(buf,0,sizeof(buf));
    sprintf(buf,"%f",val2);
    char* tg;
    double v2 = strtod(buf,&tg);    
    ASSERT_TRUE(v2 == 1.234);
    
    //test not support type
    TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
    OLAPStatus st = read_row.convert_from(0, data, tp, _mem_pool.get());
    ASSERT_TRUE( st == OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertDatetimeToDate) {
    TabletSchema tablet_schema;
    SetTabletSchema("DatetimeColumn", "DATETIME", "REPLACE", 8, false, false, &tablet_schema);
    CreateColumnWriter(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<std::string> val_string_array;
    std::string origin_val = "2019-11-25 19:07:00";
    val_string_array.emplace_back(origin_val);
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    TabletSchema convert_tablet_schema;
    SetTabletSchema("DateColumn", "DATE", "REPLACE", 3, false, false, &convert_tablet_schema);
    CreateColumnReader(tablet_schema);
    RowCursor read_row;
    read_row.init(convert_tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(
        _col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.convert_from(0 , data, write_row.column_schema(0)->type_info(), _mem_pool.get());
    std::string dest_string = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
    ASSERT_TRUE(strncmp(dest_string.c_str(), "2019-11-25", strlen("2019-11-25")) == 0);
    
    //test not support type
    TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
    OLAPStatus st = read_row.convert_from(0, data, tp, _mem_pool.get());
    ASSERT_TRUE( st == OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertDateToDatetime) {
    TabletSchema tablet_schema;
    SetTabletSchema("DateColumn", "DATE", "REPLACE", 3, false, false, &tablet_schema);
    CreateColumnWriter(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<std::string> val_string_array;
    std::string origin_val = "2019-12-04";
    val_string_array.emplace_back(origin_val);
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header_message;
    ASSERT_EQ(_column_writer->finalize(&header_message), OLAP_SUCCESS);

    TabletSchema convert_tablet_schema;
    SetTabletSchema("DateTimeColumn", "DATETIME", "REPLACE", 8, false, false, &convert_tablet_schema);
    CreateColumnReader(tablet_schema);
    RowCursor read_row;
    read_row.init(convert_tablet_schema);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(
            _col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    read_row.convert_from(0, data, write_row.column_schema(0)->type_info(), _mem_pool.get());
    std::string dest_string = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
    ASSERT_TRUE(dest_string.compare("2019-12-04 00:00:00") == 0);

    //test not support type
    TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
    OLAPStatus st = read_row.convert_from(0, data, tp, _mem_pool.get());
    ASSERT_TRUE( st == OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertIntToDate) {
    TabletSchema tablet_schema;
    SetTabletSchema("IntColumn", "INT", "REPLACE", 4, false, false, &tablet_schema);
    CreateColumnWriter(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    int time_val = 20191205;
    write_row.set_field_content(0, reinterpret_cast<char *>(&time_val), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    TabletSchema convert_tablet_schema;
    SetTabletSchema("DateColumn", "DATE", "REPLACE", 3, false, false, &convert_tablet_schema);
    CreateColumnReader(tablet_schema);

    RowCursor read_row;
    read_row.init(convert_tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(
            _col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.convert_from(0, data, write_row.column_schema(0)->type_info(), _mem_pool.get());
    std::string dest_string = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
    ASSERT_TRUE(strncmp(dest_string.c_str(), "2019-12-05", strlen("2019-12-05")) == 0);

    //test not support type
    TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
    OLAPStatus st = read_row.convert_from(0, read_row.cell_ptr(0), tp, _mem_pool.get());
    ASSERT_TRUE( st == OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertVarcharToDate) {
    TabletSchema tablet_schema;
    SetTabletSchema("VarcharColumn", "VARCHAR", "REPLACE", 255, false, false, &tablet_schema);
    CreateColumnWriter(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    // test valid format convert
    std::vector<Slice> valid_src_strs = {
        "2019-12-17",
        "19-12-17",
        "20191217",
        "191217",
        "2019/12/17",
        "19/12/17",
    };
    std::string expected_val("2019-12-17");
    for (auto src_str : valid_src_strs) {
        write_row.set_field_content(0, reinterpret_cast<char*>(&src_str), _mem_pool.get());
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

        ColumnDataHeaderMessage header;
        ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

        // because file_helper is reused in this case, we should close it.
        helper.close();
        TabletSchema convert_tablet_schema;
        SetTabletSchema("DateColumn", "DATE", "REPLACE", 3, false, false, &convert_tablet_schema);
        CreateColumnReader(tablet_schema);
        RowCursor read_row;
        read_row.init(convert_tablet_schema);

        _col_vector.reset(new ColumnVector());
        ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
        char *data = reinterpret_cast<char *>(_col_vector->col_data());
        read_row.convert_from(0, data, write_row.column_schema(0)->type_info(), _mem_pool.get());
        std::string dst_str = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
        ASSERT_EQ(expected_val, dst_str);
    }
    helper.close();
    TabletSchema convert_tablet_schema;
    SetTabletSchema("DateColumn", "DATE", "REPLACE", 3, false, false, &convert_tablet_schema);
    CreateColumnReader(tablet_schema);
    RowCursor read_row;
    read_row.init(convert_tablet_schema);

    //test not support type
    TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
    OLAPStatus st = read_row.convert_from(0, read_row.cell_ptr(0), tp, _mem_pool.get());
    ASSERT_EQ(st, OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertVarcharToTinyInt) {
    test_convert_from_varchar("TINYINT", 1, "127", OLAP_SUCCESS);
    test_convert_from_varchar("TINYINT", 1, "128", OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertVarcharToSmallInt) {
    test_convert_from_varchar("SMALLINT", 2, "32767", OLAP_SUCCESS);
    test_convert_from_varchar("SMALLINT", 2, "32768", OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertVarcharToInt) {
    test_convert_from_varchar("INT", 4, "2147483647", OLAP_SUCCESS);
    test_convert_from_varchar("INT", 4, "2147483648", OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertVarcharToBigInt) {
    test_convert_from_varchar("BIGINT", 8, "9223372036854775807", OLAP_SUCCESS);
    test_convert_from_varchar("BIGINT", 8, "9223372036854775808", OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertVarcharToLargeInt) {
    test_convert_from_varchar("LARGEINT", 16, "170141183460469000000000000000000000000", OLAP_SUCCESS);
    test_convert_from_varchar("LARGEINT", 16, "1701411834604690000000000000000000000000", OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertVarcharToFloat) {
    test_convert_from_varchar("FLOAT", 4, "3.40282e+38", OLAP_SUCCESS); 
    test_convert_from_varchar("FLOAT", 4, "1797690000000000063230304921389426434930330364336853362154109832891264341489062899406152996321966094455338163203127744334848599000464911410516510916727344709727599413825823048028128827530592629736371829425359826368844446113768685826367454055532068818593409163400929532301499014067384276511218551077374242324480.999", OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertVarcharToDouble) {
    test_convert_from_varchar("DOUBLE", 8,
            "123.456", OLAP_SUCCESS);
    test_convert_from_varchar("DOUBLE", 8,
            "1797690000000000063230304921389426434930330364336853362154109832891264341489062899406152996321966094455338163203127744334848599000464911410516510916727344709727599413825823048028128827530592629736371829425359826368844446113768685826367454055532068818593409163400929532301499014067384276511218551077374242324480.0000000000", OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertTinyIntToVarchar) {
    test_convert_to_varchar<int8_t>("TINYINT", 1, 127, "127", OLAP_SUCCESS);
}

TEST_F(TestColumn, ConvertSmallIntToVarchar) {
    test_convert_to_varchar<int16_t>("SMALLINT", 2, 32767, "32767", OLAP_SUCCESS);
}

TEST_F(TestColumn, ConvertIntToVarchar) {
    test_convert_to_varchar<int32_t>("INT", 4, 2147483647, "2147483647", OLAP_SUCCESS);
}

TEST_F(TestColumn, ConvertBigIntToVarchar) {
    test_convert_to_varchar<int64_t>("BIGINT", 8, 9223372036854775807, "9223372036854775807", OLAP_SUCCESS);
}

TEST_F(TestColumn, ConvertLargeIntToVarchar) {
    test_convert_to_varchar<int128_t>("LARGEINT", 16, 1701411834604690, "1701411834604690", OLAP_SUCCESS);
}

TEST_F(TestColumn, ConvertFloatToVarchar) {
    test_convert_to_varchar<float>("FLOAT", 4, 3.40282e+38, "3.40282e+38", OLAP_SUCCESS);
}

TEST_F(TestColumn, ConvertDoubleToVarchar) {
    test_convert_to_varchar<double>("DOUBLE", 8, 123.456, "123.456", OLAP_SUCCESS);
}

TEST_F(TestColumn, ConvertDecimalToVarchar) {
    decimal12_t val(456, 789000000);
    test_convert_to_varchar<decimal12_t>("Decimal", 12, val, "456.789000000", OLAP_SUCCESS);
}
}

int main(int argc, char** argv) {
    std::string conf_file = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conf_file.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    int ret = doris::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
    return ret;
}
