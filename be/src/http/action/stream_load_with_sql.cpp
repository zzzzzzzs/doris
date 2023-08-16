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

#include "http/action/stream_load_with_sql.h"

#include <cstddef>
#include <deque>
#include <future>
#include <shared_mutex>
#include <sstream>

// use string iequal
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/http.h>
#include <rapidjson/prettywriter.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/utils.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "http/http_channel.h"
#include "http/http_common.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/utils.h"
#include "io/fs/stream_load_pipe.h"
#include "olap/storage_engine.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/stream_load_recorder.h"
#include "util/byte_buffer.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/load_util.h"
#include "util/metrics.h"
#include "util/string_util.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/uid_util.h"

// TODO The functions in this file need to be improved
// 在 on_header 中需要获取到 plan_fragment，因此需要读取文件
namespace doris {
using namespace ErrorCode;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_with_sql_requests_total, MetricUnit::REQUESTS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_with_sql_duration_ms, MetricUnit::MILLISECONDS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(streaming_load_with_sql_current_processing,
                                   MetricUnit::REQUESTS);

StreamLoadWithSqlAction::StreamLoadWithSqlAction(ExecEnv* exec_env) : _exec_env(exec_env) {
    std::cout << "StreamLoadWithSqlAction func..." << std::endl;
    _stream_load_with_sql_entity =
            DorisMetrics::instance()->metric_registry()->register_entity("stream_load_with_sql");
    INT_COUNTER_METRIC_REGISTER(_stream_load_with_sql_entity,
                                streaming_load_with_sql_requests_total);
    INT_COUNTER_METRIC_REGISTER(_stream_load_with_sql_entity, streaming_load_with_sql_duration_ms);
    INT_GAUGE_METRIC_REGISTER(_stream_load_with_sql_entity,
                              streaming_load_with_sql_current_processing);
}

StreamLoadWithSqlAction::~StreamLoadWithSqlAction() {
    std::cout << "~StreamLoadWithSqlAction func..." << std::endl;
    DorisMetrics::instance()->metric_registry()->deregister_entity(_stream_load_with_sql_entity);
}

void StreamLoadWithSqlAction::handle(HttpRequest* req) {
    std::cout << "handle func..." << std::endl;
    std::shared_ptr<StreamLoadContext> ctx =
            std::static_pointer_cast<StreamLoadContext>(req->handler_ctx());
    if (ctx == nullptr) {
        return;
    }

    // status already set to fail
    if (ctx->status.ok()) {
        ctx->status = _handle(req, ctx);
        if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
            LOG(WARNING) << "handle streaming load failed, id=" << ctx->id
                         << ", errmsg=" << ctx->status;
        }
    }
    ctx->load_cost_millis = UnixMillis() - ctx->start_millis;

    if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
        if (ctx->body_sink != nullptr) {
            ctx->body_sink->cancel(ctx->status.to_string());
        }
    }

    if (!ctx->status.ok()) {
        auto str = std::string(ctx->to_json());
        // add new line at end
        str = str + '\n';
        HttpChannel::send_reply(req, str);
        return;
    }
    auto str = std::string(ctx->to_json());
    // add new line at end
    str = str + '\n';
    HttpChannel::send_reply(req, str);
    if (config::enable_stream_load_record) {
        str = ctx->prepare_stream_load_record(str);
        _save_stream_load_record(ctx, str);
    }
    // update statistics
    streaming_load_with_sql_requests_total->increment(1);
    streaming_load_with_sql_duration_ms->increment(ctx->load_cost_millis);
    streaming_load_with_sql_current_processing->increment(-1);
}

Status StreamLoadWithSqlAction::_handle(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx) {
    std::cout << "_handle func..." << std::endl;
    // wait stream load finish
    RETURN_IF_ERROR(ctx->body_sink->finish());
    std::cout << "wait stream load finish..." << std::endl;
    // RETURN_IF_ERROR(ctx->future.get());
    // If put file success we need commit this load
    int64_t commit_and_publish_start_time = MonotonicNanos();
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx.get()));
    ctx->commit_and_publish_txn_cost_nanos = MonotonicNanos() - commit_and_publish_start_time;
    std::cout << "success stream load..." << std::endl;
    return Status::OK();
}

int StreamLoadWithSqlAction::on_header(HttpRequest* req) {
    std::cout << "on_header func..." << std::endl;
    streaming_load_with_sql_current_processing->increment(1);

    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    req->set_handler_ctx(ctx);

    ctx->load_type = TLoadType::MANUL_LOAD;
    ctx->load_src_type = TLoadSourceType::RAW;

    ctx->label = req->header(HTTP_LABEL_KEY);
    if (ctx->label.empty()) {
        ctx->label = generate_uuid_string();
    }



    ctx->two_phase_commit = req->header(HTTP_TWO_PHASE_COMMIT) == "true" ? true : false;

    LOG(INFO) << "new income streaming load request." << ctx->brief()
              << " sql : " << req->header(HTTP_SQL);

    auto st = _on_header(req, ctx);
    if (!st.ok()) {
        ctx->status = std::move(st);
        if (ctx->body_sink != nullptr) {
            ctx->body_sink->cancel(ctx->status.to_string());
        }
        auto str = ctx->to_json();
        // add new line at end
        str = str + '\n';
        HttpChannel::send_reply(req, str);
        streaming_load_with_sql_current_processing->increment(-1);
        if (config::enable_stream_load_record) {
            str = ctx->prepare_stream_load_record(str);
            _save_stream_load_record(ctx, str);
        }
        return -1;
    }
    return 0;
}

// TODO The parameters of this function may need to be refactored because the parameters in HttpRequest are not sufficient.
Status StreamLoadWithSqlAction::_on_header(HttpRequest* http_req,
                                           std::shared_ptr<StreamLoadContext> ctx) {
    std::cout << "_on_header func..." << std::endl;
    ctx->db = "test";
    ctx->table = "t1";
    // auth information
    if (!parse_basic_auth(*http_req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
        return Status::InternalError("no valid Basic authorization");
    }


    // check content length
    ctx->body_bytes = 0;
    // size_t csv_max_body_bytes = config::streaming_load_max_mb * 1024 * 1024;
    // size_t json_max_body_bytes = config::streaming_load_json_max_mb * 1024 * 1024;
    // bool read_json_by_line = false;

    auto pipe = std::make_shared<io::StreamLoadPipe>(
            io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
            ctx->body_bytes /* total_length */);
    ctx->body_sink = pipe;
    ctx->pipe = pipe;

    auto scheme_pipe = std::make_shared<io::StreamLoadPipe>(
            io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
            ctx->body_bytes /* total_length */);
    ctx->scheme_body_sink = scheme_pipe;
    ctx->scheme_pipe = scheme_pipe;

    RETURN_IF_ERROR(_exec_env->new_load_stream_mgr()->put(ctx->id, ctx));

    // begin transaction
    int64_t begin_txn_start_time = MonotonicNanos();
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->begin_txn(ctx.get()));
    ctx->begin_txn_cost_nanos = MonotonicNanos() - begin_txn_start_time;

    // process put file
    return _process_put(http_req, ctx);
}

void StreamLoadWithSqlAction::on_chunk_data(HttpRequest* req) {
    std::cout << "on_chunk_data func..." << std::endl;
    std::shared_ptr<StreamLoadContext> ctx =
            std::static_pointer_cast<StreamLoadContext>(req->handler_ctx());
    if (ctx == nullptr || !ctx->status.ok()) {
        return;
    }

    struct evhttp_request* ev_req = req->get_evhttp_request();
    auto evbuf = evhttp_request_get_input_buffer(ev_req);

    int64_t start_read_data_time = MonotonicNanos();
    while (evbuffer_get_length(evbuf) > 0) {
        auto bb = ByteBuffer::allocate(128 * 1024);
        auto remove_bytes = evbuffer_remove(evbuf, bb->ptr, bb->capacity);
        bb->pos = remove_bytes;
        bb->flip();
        std::cout << "Data in bb: " << std::string(bb->ptr, remove_bytes) << std::endl;
        auto st = ctx->body_sink->append(bb);
        ctx->scheme_body_sink->append(bb);
        if (!st.ok()) {
            LOG(WARNING) << "append body content failed. errmsg=" << st << ", " << ctx->brief();
            ctx->status = st;
            return;
        }
        ctx->receive_bytes += remove_bytes;
    }
    ctx->scheme_body_sink->finish();
    ctx->read_data_cost_nanos += (MonotonicNanos() - start_read_data_time);
}

void StreamLoadWithSqlAction::free_handler_ctx(std::shared_ptr<void> param) {
    std::cout << "free_handler_ctx func..." << std::endl;
    std::shared_ptr<StreamLoadContext> ctx = std::static_pointer_cast<StreamLoadContext>(param);
    if (ctx == nullptr) {
        return;
    }
    // sender is gone, make receiver know it
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel("sender is gone");
    }
    // remove stream load context from stream load manager and the resource will be released
    ctx->exec_env()->new_load_stream_mgr()->remove(ctx->id);
}

Status StreamLoadWithSqlAction::_process_put(HttpRequest* http_req,
                                             std::shared_ptr<StreamLoadContext> ctx) {
    std::cout << "_process_put func..." << std::endl;

    TStreamLoadPutRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.formatType = ctx->format;
    request.__set_load_sql(http_req->header(HTTP_SQL));
    request.__set_compress_type(ctx->compress_type);
    request.__set_header_type(ctx->header_type);
    request.__set_loadId(ctx->id.to_thrift());
    request.fileType = TFileType::FILE_STREAM;
    request.__set_label(ctx->label);
    if (_exec_env->master_info()->__isset.backend_id) {
        request.__set_backend_id(_exec_env->master_info()->backend_id);
    } else {
        LOG(WARNING) << "_exec_env->master_info not set backend_id";
    }

    // plan this load
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    int64_t stream_load_put_start_time = MonotonicNanos();
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, ctx](FrontendServiceConnection& client) {
                client->streamLoadPut(ctx->put_result, request);
            }));
    ctx->stream_load_put_cost_nanos = MonotonicNanos() - stream_load_put_start_time;
    Status plan_status(Status::create(ctx->put_result.status));
    if (!plan_status.ok()) {
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status << ctx->brief();
        return plan_status;
    }
    return Status::OK();
}

Status StreamLoadWithSqlAction::_data_saved_path(HttpRequest* req, std::string* file_path) {
    std::cout << "_data_saved_path func..." << std::endl;
    std::string prefix;
    RETURN_IF_ERROR(
            _exec_env->load_path_mgr()->allocate_dir("stream_load_local_file", "", &prefix));
    timeval tv;
    gettimeofday(&tv, nullptr);
    struct tm tm;
    time_t cur_sec = tv.tv_sec;
    localtime_r(&cur_sec, &tm);
    char buf[64];
    strftime(buf, 64, "%Y%m%d%H%M%S", &tm);
    std::stringstream ss;
    ss << prefix << buf << "." << tv.tv_usec;
    *file_path = ss.str();
    return Status::OK();
}

void StreamLoadWithSqlAction::_save_stream_load_record(std::shared_ptr<StreamLoadContext> ctx,
                                                       const std::string& str) {
    std::cout << "_save_stream_load_record func..." << std::endl;
    auto stream_load_recorder = StorageEngine::instance()->get_stream_load_recorder();
    if (stream_load_recorder != nullptr) {
        std::string key =
                std::to_string(ctx->start_millis + ctx->load_cost_millis) + "_" + ctx->label;
        auto st = stream_load_recorder->put(key, str);
        if (st.ok()) {
            LOG(INFO) << "put stream_load_record rocksdb successfully. label: " << ctx->label
                      << ", key: " << key;
        }
    } else {
        LOG(WARNING) << "put stream_load_record rocksdb failed. stream_load_recorder is null.";
    }
}

} // namespace doris
