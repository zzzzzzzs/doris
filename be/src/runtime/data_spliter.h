// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_RUNTIME_DATA_SPLITER_H
#define BDG_PALO_BE_RUNTIME_DATA_SPLITER_H

#include <unordered_map>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/data_sink.h"
#include "runtime/dpp_sink_internal.h"
#include "util/runtime_profile.h"

namespace palo {

class RowDescriptor;
class RuntimeState;
class RowBatch;
class ObjectPool;
class TDataSplitSink;
class Expr;
class ExprContext;
class PartitionInfo;
class RollupSchema;
class TupleRow;
class DppSink;
class MemTracker;


// DataSpliter used to split data input to groups of data
// according to partition information, distributed information
// and rollup information.
// TODO(zc): think about this to make it more reusable
class DataSpliter : public DataSink {
public:
    // Construct from thrift struct which is generated by FE.
    DataSpliter(const RowDescriptor& row_desc);

    virtual ~DataSpliter();

    virtual Status prepare(RuntimeState* state);

    virtual Status open(RuntimeState* state);

    virtual Status send(RuntimeState* state, RowBatch* batch);

    virtual Status close(RuntimeState* state, Status close_status);

    // Returns the runtime profile for the sink.
    virtual RuntimeProfile* profile() {
        return _profile;
    }

    static Status from_thrift(ObjectPool* pool,
                              const TDataSplitSink& t_sink,
                              DataSpliter* spliter);

private:
    int binary_find_partition(const PartRangeKey& key) const;
    Status process_partition(
            RuntimeState* state, TupleRow* row, PartitionInfo** info, int32_t* part_index);
    Status process_distribute(RuntimeState* state, TupleRow* row,
                              const PartitionInfo* part, uint32_t* mod);
    Status send_row(
            RuntimeState* state, const TabletDesc& desc, TupleRow* row, DppSink* dpp_sink);
    Status process_one_row(RuntimeState* state, TupleRow* row);

    boost::scoped_ptr<ObjectPool> _obj_pool;
    const RowDescriptor& _row_desc;

    // Information used to partition data
    // partition exprs
    std::vector<ExprContext*> _partition_expr_ctxs;

    // map from range value to partition_id
    // sorted in ascending orderi by range for binary search
    std::vector<PartitionInfo*> _partition_infos;

    // Information of rollup
    // from name to rollup information.
    std::map<std::string, RollupSchema*> _rollup_map;

    std::unordered_map<TabletDesc, RowBatch*> _batch_map;
    std::unordered_map<TabletDesc, DppSink*> _sink_map;

    std::vector<DppSink*> _dpp_sink_vec;

    // Allocated from _pool
    RuntimeProfile* _profile;

    // Wall time senders spend waiting for the recv buffer to have capacity.
    RuntimeProfile::Counter* _split_timer;
    RuntimeProfile::Counter* _finish_timer;
};

}

#endif
