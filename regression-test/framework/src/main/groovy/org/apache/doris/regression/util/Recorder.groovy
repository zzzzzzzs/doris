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

package org.apache.doris.regression.util

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.doris.regression.suite.ScriptInfo
import org.apache.doris.regression.suite.SuiteInfo

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@CompileStatic
class Recorder {
    public final List<SuiteInfo> successList = new Vector<>()
    public final List<SuiteInfo> failureList = new Vector<>()
    public final List<String> skippedList = new Vector<>()
    public final List<ScriptInfo> fatalScriptList = new Vector<>()

    public static AtomicLong failureCounter = new AtomicLong(0);

    public static boolean isFailureExceedLimit(int limit) {
        if (limit <=0) {
            return false;
        }
        return failureCounter.get() > limit;
    }

    public static int getFailureOrFatalNum() {
        return failureCounter.get()
    }

    public int getFatalNum() {
        return fatalScriptList.size()
    }

    void onSuccess(SuiteInfo suiteInfo) {
        successList.add(suiteInfo)
    }

    void onFailure(SuiteInfo suiteInfo) {
        failureList.add(suiteInfo)
        failureCounter.incrementAndGet();
    }

    void onSkip(String skippedFile) {
        skippedList.add(skippedFile)
    }

    void onFatal(ScriptInfo scriptInfo) {
        fatalScriptList.add(scriptInfo)
        failureCounter.incrementAndGet();
    }
}
