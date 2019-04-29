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

#include "exec/parquet_scanner.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "runtime/tuple.h"
#include "exec/parquet_reader.h"
#include "exprs/expr.h"
#include "exec/text_converter.h"
#include "exec/text_converter.hpp"
#include "exec/local_file_reader.h"
#include "exec/broker_reader.h"
#include "exec/decompressor.h"
#include "exec/parquet_reader.h"

namespace doris {

ParquetScanner::ParquetScanner(RuntimeState* state,
                             RuntimeProfile* profile,
                             const TBrokerScanRangeParams& params,
                             const std::vector<TBrokerRangeDesc>& ranges,
                             const std::vector<TNetworkAddress>& broker_addresses,
                             ScannerCounter* counter) :
        _state(state),
        _profile(profile),
        _params(params),
        _ranges(ranges),
        _broker_addresses(broker_addresses),
        // _splittable(params.splittable),
        _cur_file_reader(nullptr),
        _next_range(0),
        _cur_line_reader_eof(false),
        _scanner_eof(false),
        _src_tuple(nullptr),
        _src_tuple_row(nullptr),
#if BE_TEST
        _mem_tracker(new MemTracker()),
        _mem_pool(_mem_tracker.get()),
#else
        _mem_tracker(new MemTracker(-1, "Broker Scanner", state->instance_mem_tracker())),
        _mem_pool(_state->instance_mem_tracker()),
#endif
        _dest_tuple_desc(nullptr),
        _counter(counter),
        _rows_read_counter(nullptr),
        _read_timer(nullptr),
        _materialize_timer(nullptr) {
}

ParquetScanner::~ParquetScanner() {
    close();
}

Status ParquetScanner::init_expr_ctxes() {
    // Constcut _src_slot_descs
    const TupleDescriptor* src_tuple_desc =
        _state->desc_tbl().get_tuple_descriptor(_params.src_tuple_id);
    if (src_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown source tuple descriptor, tuple_id=" << _params.src_tuple_id;
        return Status(ss.str());
    }

    std::map<SlotId, SlotDescriptor*> src_slot_desc_map;
    for (auto slot_desc : src_tuple_desc->slots()) {
        src_slot_desc_map.emplace(slot_desc->id(), slot_desc);
    }
    for (auto slot_id : _params.src_slot_ids) {
        auto it = src_slot_desc_map.find(slot_id);
        if (it == std::end(src_slot_desc_map)) {
            std::stringstream ss;
            ss << "Unknown source slot descriptor, slot_id=" << slot_id;
            return Status(ss.str());
        }
        _src_slot_descs.emplace_back(it->second);
    }
    // Construct source tuple and tuple row
    _src_tuple = (Tuple*) _mem_pool.allocate(src_tuple_desc->byte_size());
    _src_tuple_row = (TupleRow*) _mem_pool.allocate(sizeof(Tuple*));
    _src_tuple_row->set_tuple(0, _src_tuple);
    _row_desc.reset(new RowDescriptor(_state->desc_tbl(),
                                      std::vector<TupleId>({_params.src_tuple_id}),
                                      std::vector<bool>({false})));

    // Construct dest slots information
    _dest_tuple_desc = _state->desc_tbl().get_tuple_descriptor(_params.dest_tuple_id);
    if (_dest_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown dest tuple descriptor, tuple_id=" << _params.dest_tuple_id;
        return Status(ss.str());
    }

    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        auto it = _params.expr_of_dest_slot.find(slot_desc->id());
        if (it == std::end(_params.expr_of_dest_slot)) {
            std::stringstream ss;
            ss << "No expr for dest slot, id=" << slot_desc->id()
                << ", name=" << slot_desc->col_name();
            return Status(ss.str());
        }
        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(_state->obj_pool(), it->second, &ctx));
        RETURN_IF_ERROR(ctx->prepare(_state, *_row_desc.get(), _mem_tracker.get()));
        RETURN_IF_ERROR(ctx->open(_state));
        _dest_expr_ctx.emplace_back(ctx);
    }

    return Status::OK;
}

Status ParquetScanner::open() {
    RETURN_IF_ERROR(init_expr_ctxes());

    _rows_read_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _read_timer = ADD_TIMER(_profile, "TotalRawReadTime(*)");
    _materialize_timer = ADD_TIMER(_profile, "MaterializeTupleTime(*)");

    return Status::OK;
}

Status ParquetScanner::get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof) {
    SCOPED_TIMER(_read_timer);
    // Get one line
    while (!_scanner_eof) {
        if (_cur_file_reader == nullptr || _cur_line_reader_eof) {
            RETURN_IF_ERROR(open_next_reader());
            // If there isn't any more reader, break this
            if (_scanner_eof) {
                continue;
            }
        }
        RETURN_IF_ERROR(_cur_file_reader->read(_src_tuple, _src_slot_descs, tuple_pool, &_cur_line_reader_eof));
        if (_cur_line_reader_eof) {
            continue; // process current file over.
        }

        {
            COUNTER_UPDATE(_rows_read_counter, 1);
            SCOPED_TIMER(_materialize_timer);
            if (fill_dest_tuple(tuple, tuple_pool)) {
                break;// break iff true
            }
        }
    }
    if (_scanner_eof) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK;
}

Status ParquetScanner::open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK;
    }

    RETURN_IF_ERROR(open_file_reader());
    _next_range++;

    return Status::OK;
}

Status ParquetScanner::open_file_reader() {
    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
            _cur_file_reader = nullptr;
        } else {
            delete _cur_file_reader;
            _cur_file_reader = nullptr;
        }
    }
    const TBrokerRangeDesc& range = _ranges[_next_range];
    switch (range.file_type) {
        case TFileType::FILE_LOCAL: {
            FileReader *file_reader = new LocalFileReader(range.path, range.start_offset);
            RETURN_IF_ERROR(file_reader->open());
            _cur_file_reader = new ParquetReaderWrap(file_reader);
            _cur_file_reader->inital_parquet_reader();
            break;
        }
        case TFileType::FILE_BROKER: {
            FileReader *file_reader = new BrokerReader(_state->exec_env(), _broker_addresses, _params.properties, range.path, range.start_offset);
            RETURN_IF_ERROR(file_reader->open());
            _cur_file_reader = new ParquetReaderWrap(file_reader);
            _cur_file_reader->inital_parquet_reader();
            break;
        }
#if 0
        case TFileType::FILE_STREAM:
        {
            _stream_load_pipe = _state->exec_env()->load_stream_mgr()->get(range.load_id);
            if (_stream_load_pipe == nullptr) {
                return Status("unknown stream load id");
            }
            _cur_file_reader = _stream_load_pipe.get();
            break;
        }
#endif
        default: {
            std::stringstream ss;
            ss << "Unknown file type, type=" << range.file_type;
            return Status(ss.str());
        }
    }
    return Status::OK;
}

void ParquetScanner::close() {
    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
            _cur_file_reader = nullptr;
        } else {
            delete _cur_file_reader;
            _cur_file_reader = nullptr;
        }
    }
    Expr::close(_dest_expr_ctx, _state);
}

bool ParquetScanner::fill_dest_tuple(Tuple* dest_tuple, MemPool* mem_pool) {
    int ctx_idx = 0;
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        ExprContext* ctx = _dest_expr_ctx[ctx_idx++];
        void* value = ctx->get_value(_src_tuple_row);
        if (value == nullptr) {
            if (slot_desc->is_nullable()) {
                dest_tuple->set_null(slot_desc->null_indicator_offset());
                continue;
            } else {
                std::stringstream error_msg;
                error_msg << "column(" << slot_desc->col_name() << ") value is null";
                _state->append_error_msg_to_file("", error_msg.str());
                _counter->num_rows_filtered++;
                LOG(WARNING) << error_msg.str();
                return false;
            }
        }
        dest_tuple->set_not_null(slot_desc->null_indicator_offset());
        void* slot = dest_tuple->get_slot(slot_desc->tuple_offset());
        RawValue::write(value, slot, slot_desc->type(), mem_pool);
    }
    return true;
}

}
