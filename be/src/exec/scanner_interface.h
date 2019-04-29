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

#ifndef BE_SRC_EXEC_SCANNER_INTERFACE_H_
#define BE_SRC_EXEC_SCANNER_INTERFACE_H_

#include "common/status.h"
#include "runtime/tuple.h"

namespace doris {

struct ScannerCounter {
    ScannerCounter() :
        num_rows_total(0),
        // num_rows_returned(0),
        num_rows_filtered(0),
        num_rows_unselected(0) {
    }

    int64_t num_rows_total; // total read rows (read from source)
    // int64_t num_rows_returned;  // qualified rows (match the dest schema)
    int64_t num_rows_filtered;  // unqualified rows (unmatch the dest schema, or no partition)
    int64_t num_rows_unselected; // rows filterd by predicates
};
class ScannerInterface {
public:
    virtual ~ScannerInterface() {
        // do nothing
    };

    // Open this scanner, will initialize information need to
    virtual Status open() = 0;

    // Get next tuple
    virtual Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof) = 0;

    // Close this scanner
    virtual void close() = 0;
};

} /* namespace doris */

#endif /* BE_SRC_EXEC_SCANNER_INTERFACE_H_ */
