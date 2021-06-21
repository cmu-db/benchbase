/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.twitter;

import com.oltpbenchmark.WorkloadConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;

public class TwitterConfiguration {

    private final XMLConfiguration xmlConfig;

    public TwitterConfiguration(WorkloadConfiguration workConf) {
        this.xmlConfig = workConf.getXmlConfig();
    }

    public String getTracefile() {
        return xmlConfig.getString("tracefile", null);
    }

    public String getTracefile2() {
        return xmlConfig.getString("tracefile2", null);
    }
}
