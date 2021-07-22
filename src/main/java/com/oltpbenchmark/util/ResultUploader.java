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

package com.oltpbenchmark.util;

import com.oltpbenchmark.Results;
import com.oltpbenchmark.api.collectors.DBParameterCollector;
import com.oltpbenchmark.api.collectors.DBParameterCollectorGen;
import com.oltpbenchmark.types.DatabaseType;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;

import java.io.PrintStream;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

public class ResultUploader {

    private static final String[] IGNORE_CONF = {
            "type",
            "driver",
            "url",
            "username",
            "password"
    };

    private static final String[] BENCHMARK_KEY_FIELD = {
            "isolation",
            "scalefactor",
            "terminals"
    };

    private final XMLConfiguration expConf;
    private final DBParameterCollector collector;
    private final Results results;
    private final DatabaseType dbType;
    private final String benchType;


    public ResultUploader(Results r, XMLConfiguration conf, CommandLine argsLine) {
        this.expConf = conf;
        this.results = r;
        this.dbType = DatabaseType.valueOf(expConf.getString("type"));
        this.benchType = argsLine.getOptionValue("b");

        String dbUrl = expConf.getString("url");
        String username = expConf.getString("username");
        String password = expConf.getString("password");


        this.collector = DBParameterCollectorGen.getCollector(dbType, dbUrl, username, password);

    }

    public void writeDBParameters(PrintStream os) {
        String dbConf = collector.collectParameters();
        os.print(dbConf);
    }

    public void writeDBMetrics(PrintStream os) {
        os.print(collector.collectMetrics());
    }

    public void writeBenchmarkConf(PrintStream os) throws ConfigurationException {

        XMLConfiguration outputConf = (XMLConfiguration) expConf.clone();
        for (String key : IGNORE_CONF) {
            outputConf.clearProperty(key);
        }

        FileHandler handler = new FileHandler(outputConf);
        handler.save(os);
    }

    public void writeSummary(PrintStream os) {
        Map<String, Object> summaryMap = new TreeMap<>();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Date now = new Date();
        summaryMap.put("Current Timestamp (milliseconds)", now.getTime());
        summaryMap.put("DBMS Type", dbType);
        summaryMap.put("DBMS Version", collector.collectVersion());
        summaryMap.put("Benchmark Type", benchType);
        summaryMap.put("Latency Distribution", results.getDistributionStatistics().toMap());
        summaryMap.put("Throughput (requests/second)", results.requestsPerSecond());
        for (String field : BENCHMARK_KEY_FIELD) {
            summaryMap.put(field, expConf.getString(field));
        }
        os.println(JSONUtil.format(JSONUtil.toJSONString(summaryMap)));
    }


}
