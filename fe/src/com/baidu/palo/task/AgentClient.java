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

package com.baidu.palo.task;

import com.baidu.palo.common.ClientPool;
import com.baidu.palo.common.Status;
import com.baidu.palo.thrift.BackendService;
import com.baidu.palo.thrift.TAgentResult;
import com.baidu.palo.thrift.TAgentServiceVersion;
import com.baidu.palo.thrift.TMiniLoadEtlStatusRequest;
import com.baidu.palo.thrift.TMiniLoadEtlStatusResult;
import com.baidu.palo.thrift.TMiniLoadEtlTaskRequest;
import com.baidu.palo.thrift.TDeleteEtlFilesRequest;
import com.baidu.palo.thrift.TExportStatusResult;
import com.baidu.palo.thrift.TExportTaskRequest;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TSnapshotRequest;
import com.baidu.palo.thrift.TStatus;
import com.baidu.palo.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AgentClient {
    private static final Logger LOG = LogManager.getLogger(AgentClient.class);

    private String host;
    private int port;
    
    private BackendService.Client client;
    private TNetworkAddress address;
    private boolean ok;

    public AgentClient(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public TAgentResult submitEtlTask(TMiniLoadEtlTaskRequest request) {
        TAgentResult result = null;
        LOG.debug("submit etl task. request: {}", request);
        try {
            borrowClient();
            // submit etl task
            result = client.submit_etl_task(request);
            ok = true;
        } catch (Exception e) {
            LOG.warn("submit etl task error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public TAgentResult makeSnapshot(TSnapshotRequest request) {
        TAgentResult result = null;
        LOG.debug("submit make snapshot task. request: {}", request);
        try {
            borrowClient();
            // submit make snapshot task
            result = client.make_snapshot(request);
            ok = true;
        } catch (Exception e) {
            LOG.warn("submit make snapshot error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public TAgentResult releaseSnapshot(String snapshotPath) {
        TAgentResult result = null;
        LOG.debug("submit release snapshot task. snapshotPath: {}", snapshotPath);
        try {
            borrowClient();
            // submit release snapshot task
            result = client.release_snapshot(snapshotPath);
            ok = true;
        } catch (Exception e) {
            LOG.warn("submit release snapshot error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public Status submitExportTask(TExportTaskRequest request) {
        Status result = Status.CANCELLED;
        LOG.debug("submit export task. request: {}", request);
        try {
            borrowClient();
            // submit export task
            TStatus status = client.submit_export_task(request);
            result = new Status(status);
        } catch (Exception e) {
            LOG.warn("submit export task error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public TMiniLoadEtlStatusResult getEtlStatus(long jobId, long taskId) {
        TMiniLoadEtlStatusResult result = null;
        TMiniLoadEtlStatusRequest request = new TMiniLoadEtlStatusRequest(TAgentServiceVersion.V1, 
                new TUniqueId(jobId, taskId));
        LOG.debug("get mini load etl task status. request: {}", request);
        try {
            borrowClient();
            // get etl status
            result = client.get_etl_status(request);
            ok = true;
        } catch (Exception e) {
            LOG.warn("get etl status error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public TExportStatusResult getExportStatus(long jobId, long taskId) {
        TExportStatusResult result = null;
        TUniqueId request = new TUniqueId(jobId, taskId);
        LOG.debug("get export task status. request: {}", request);
        try {
            borrowClient();
            // get export status
            result = client.get_export_status(request);
            ok = true;
        } catch (Exception e) {
            LOG.warn("get export status error", e);
        } finally {
            returnClient();
        }
        return result;
    }
    
    public Status eraseExportTask(long jobId, long taskId) {
        Status result = Status.CANCELLED;
        TUniqueId request = new TUniqueId(jobId, taskId);
        LOG.debug("erase export task. request: {}", request);
        try {
            borrowClient();
            // erase export task
            TStatus status = client.erase_export_task(request);
            result = new Status(status);
        } catch (Exception e) {
            LOG.warn("submit export task error", e);
        } finally {
            returnClient();
        }
        return result;
    }

    public void deleteEtlFiles(long dbId, long jobId, String dbName, String label) {
        TDeleteEtlFilesRequest request = new TDeleteEtlFilesRequest(TAgentServiceVersion.V1, 
                new TUniqueId(dbId, jobId), dbName, label);
        LOG.debug("delete etl files. request: {}", request);
        try {
            borrowClient();
            // delete etl files
            client.delete_etl_files(request);
            ok = true;
        } catch (Exception e) {
            LOG.warn("delete etl files error", e);
        } finally {
            returnClient();
        }
    }
    
    private void borrowClient() throws Exception {
        // create agent client
        ok = false;
        address = new TNetworkAddress(host, port);
        client = ClientPool.backendPool.borrowObject(address);
    }
    
    private void returnClient() {
        if (ok) {
            ClientPool.backendPool.returnObject(address, client);
        } else {
            ClientPool.backendPool.invalidateObject(address, client);
        }
    }
}
