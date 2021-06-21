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

package com.oltpbenchmark.api.collectors;

import com.oltpbenchmark.util.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

public class DBCollector implements DBParameterCollector {

    protected static final Logger LOG = LoggerFactory.getLogger(DBCollector.class);

    protected final Map<String, String> dbParameters = new TreeMap<>();

    protected final Map<String, String> dbMetrics = new TreeMap<>();

    protected String version = null;

    @Override
    public boolean hasParameters() {
        return (!dbParameters.isEmpty());
    }

    @Override
    public boolean hasMetrics() {
        return (!dbMetrics.isEmpty());
    }

    @Override
    public String collectParameters() {
        return JSONUtil.format(JSONUtil.toJSONString(dbParameters));
    }

    @Override
    public String collectMetrics() {
        return JSONUtil.format(JSONUtil.toJSONString(dbMetrics));
    }

    @Override
    public String collectVersion() {
        return version;
    }


}
