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

package org.apache.doris.planner;


import org.apache.doris.analysis.ArrayLiteral;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PGroupCommitInsertRequest;
import org.apache.doris.proto.Types;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TPlan;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// Used to generate a plan fragment for a group commit
// we only support OlapTable now.
public class GroupCommitPlanner {
    private static final Logger LOG = LogManager.getLogger(GroupCommitPlanner.class);

    private Database db;
    private OlapTable table;
    private TUniqueId loadId;
    private TPlan plan;
    private DescriptorTable descTable;
    private TScanRangeParams scanRangeParam;

    public GroupCommitPlanner(Database db, OlapTable table, TUniqueId queryId) throws UserException {
        this.db = db;
        this.table = table;
        TStreamLoadPutRequest streamLoadPutRequest = new TStreamLoadPutRequest();
        streamLoadPutRequest
                .setDb(db.getFullName())
                .setMaxFilterRatio(1)
                .setTbl(table.getName())
                .setFileType(TFileType.FILE_STREAM).setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                .setMergeType(TMergeType.APPEND).setThriftRpcTimeoutMs(5000).setLoadId(queryId)
                .setGroupCommit(true);
        StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(streamLoadPutRequest);
        StreamLoadPlanner planner = new StreamLoadPlanner(db, table, streamLoadTask);
        // Will using load id as query id in fragment
        TExecPlanFragmentParams tRequest = planner.plan(streamLoadTask.getId());
        descTable = planner.getDescTable();
        TPlanFragment fragment = tRequest.getFragment();
        plan = fragment.getPlan();
        for (Map.Entry<Integer, List<TScanRangeParams>> entry : tRequest.params.per_node_scan_ranges.entrySet()) {
            for (TScanRangeParams scanRangeParams : entry.getValue()) {
                scanRangeParams.scan_range.ext_scan_range.file_scan_range.params.setFormatType(
                        TFileFormatType.FORMAT_PROTO);
                scanRangeParams.scan_range.ext_scan_range.file_scan_range.params.setCompressType(
                        TFileCompressType.PLAIN);
            }
        }
        List<TScanRangeParams> scanRangeParams = tRequest.params.per_node_scan_ranges.values().stream()
                .flatMap(Collection::stream).collect(Collectors.toList());
        Preconditions.checkState(scanRangeParams.size() == 1);
        loadId = queryId;
        scanRangeParam = scanRangeParams.get(0);
    }


    public PGroupCommitInsertRequest createPGroupCommitInsertRequest(List<InternalService.PDataRow> rows)
            throws TException {
        PGroupCommitInsertRequest request = PGroupCommitInsertRequest.newBuilder()
                .setDbId(db.getId())
                .setTableId(table.getId())
                .setDescTbl(ByteString.copyFrom(new TSerializer().serialize(descTable.toThrift())))
                .setBaseSchemaVersion(table.getBaseSchemaVersion())
                .setPlanNode(ByteString.copyFrom(new TSerializer().serialize(plan)))
                .setScanRangeParams(ByteString.copyFrom(new TSerializer().serialize(scanRangeParam)))
                .setLoadId(Types.PUniqueId.newBuilder().setHi(loadId.hi).setLo(loadId.lo)
                .build()).addAllData(rows)
                .build();
        return request;
    }

    public static InternalService.PDataRow getRowValue(List<Expr> cols) throws UserException {
        if (cols.isEmpty()) {
            return null;
        }
        InternalService.PDataRow.Builder row = InternalService.PDataRow.newBuilder();
        for (Expr expr : cols) {
            if (!expr.isConstant()) {
                throw new UserException(
                    "do not support non-constant expr in transactional insert operation: " + expr.toSql());
            }
            if (expr instanceof NullLiteral) {
                row.addColBuilder().setValue("\\N");
            } else if (expr instanceof ArrayLiteral) {
                row.addColBuilder().setValue(expr.getStringValueForArray());
            } else if (!expr.getChildren().isEmpty()) {
                expr.getChildren().forEach(child -> processExprVal(child, row));
            } else {
                row.addColBuilder().setValue(expr.getStringValue());
            }
        }
        return row.build();
    }

    private static void processExprVal(Expr expr, InternalService.PDataRow.Builder row) {
        if (expr.getChildren().isEmpty()) {
            row.addColBuilder().setValue(expr.getStringValue());
            return;
        }
        for (Expr child : expr.getChildren()) {
            processExprVal(child, row);
        }
    }

    public TPlan getPlan() {
        return plan;
    }

    public DescriptorTable getDescTable() {
        return descTable;
    }

    public TScanRangeParams getScanRangeParam() {
        return scanRangeParam;
    }
}

