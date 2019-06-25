/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.util;

import com.oltpbenchmark.Results;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.collectors.DBCollector;
import com.oltpbenchmark.types.DatabaseType;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

public class ResultUploader {
    private static final Logger LOG = Logger.getLogger(ResultUploader.class);

    private static String[] IGNORE_CONF = {
            "dbtype",
            "driver",
            "DBUrl",
            "username",
            "password",
            "uploadCode",
            "uploadUrl"
    };

    private static String[] BENCHMARK_KEY_FIELD = {
            "isolation",
            "scalefactor",
            "terminals"
    };

    XMLConfiguration expConf;
    Results results;
    CommandLine argsLine;
    DBCollector collector;

    String dbUrl;
    DatabaseType dbType;
    String username, password;
    String benchType;
//    int windowSize;
    String uploadCode, uploadUrl;
    String uploadHash;

    public ResultUploader(Results r, XMLConfiguration conf, CommandLine argsLine) {
        this.expConf = conf;
        this.results = r;
        this.argsLine = argsLine;

        dbUrl = expConf.getString("DBUrl");
        dbType = DatabaseType.get(expConf.getString("dbtype"));
        username = expConf.getString("username");
        password = expConf.getString("password");
        benchType = argsLine.getOptionValue("b");
//        windowSize = 1;
//        if (argsLine.hasOption("s")) {
//        	windowSize = Integer.parseInt(argsLine.getOptionValue("s"));
//        } else {
//        	windowSize = 1;
//        }
        uploadCode = expConf.getString("uploadCode");
        uploadUrl = expConf.getString("uploadUrl");
        uploadHash = argsLine.getOptionValue("uploadHash");
        uploadHash = uploadHash == null ? "" : uploadHash;

        this.collector = DBCollector.createCollector(dbType, dbUrl, username, password);
        assert(this.collector != null);
    }
    
    public DBCollector getConfCollector() {
        return (this.collector);
    }

    public void writeDBParameters(PrintStream os) {
        this.collector.writeParameters(os);
    }
    
    public void writeDBMetrics(PrintStream os) {
        this.collector.writeMetrics(os);
    }

    public void writeBenchmarkConf(PrintStream os) throws ConfigurationException {
        XMLConfiguration outputConf = (XMLConfiguration) expConf.clone();
        for (String key: IGNORE_CONF) {
            outputConf.clearProperty(key);
        }
        outputConf.save(os);
    }

    public void writeSummary(PrintStream os) {
        Map<String, Object> summaryMap = new TreeMap<String, Object>();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Date now = new Date();
        summaryMap.put("Current Timestamp (milliseconds)", now.getTime());
        summaryMap.put("DBMS Type", dbType.name().toLowerCase());
        summaryMap.put("DBMS Version", this.collector.getVersion());
        summaryMap.put("Benchmark Type", benchType);
        summaryMap.put("Latency Distribution", results.latencyDistribution.toMap());
        summaryMap.put("Throughput (requests/second)", results.getRequestsPerSecond());
        for (String field: BENCHMARK_KEY_FIELD) {
            summaryMap.put(field, expConf.getString(field));
        }
        os.println(JSONUtil.format(JSONUtil.toJSONString(summaryMap)));
    }

    public void uploadResult(List<TransactionType> activeTXTypes) throws ParseException {
        try {
            File expConfigFile = File.createTempFile("expconfig", ".tmp");
            File samplesFile = File.createTempFile("samples", ".tmp");
            File summaryFile = File.createTempFile("summary", ".tmp");
            File paramsFile = File.createTempFile("params", ".tmp");
            File metricsFile = File.createTempFile("metrics", ".tmp");
            File csvDataFile = File.createTempFile("csv", ".gz");

            PrintStream confOut = new PrintStream(new FileOutputStream(expConfigFile));
            writeBenchmarkConf(confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(paramsFile));
            writeDBParameters(confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(metricsFile));
            writeDBMetrics(confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(samplesFile));
            results.writeCSV2(confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(summaryFile));
            writeSummary(confOut);
            confOut.close();

            confOut = new PrintStream(new GZIPOutputStream(new FileOutputStream(csvDataFile)));
            results.writeAllCSVAbsoluteTiming(activeTXTypes, confOut);
            confOut.close();

            CloseableHttpClient httpclient = HttpClients.createDefault();
            HttpPost httppost = new HttpPost(uploadUrl);

            HttpEntity reqEntity = MultipartEntityBuilder.create()
                    .addTextBody("upload_code", uploadCode)
                    .addTextBody("upload_hash", uploadHash)
                    .addPart("sample_data", new FileBody(samplesFile))
                    .addPart("raw_data", new FileBody(csvDataFile))
                    .addPart("db_parameters_data", new FileBody(paramsFile))
                    .addPart("db_metrics_data", new FileBody(metricsFile))
                    .addPart("benchmark_conf_data", new FileBody(expConfigFile))
                    .addPart("summary_data", new FileBody(summaryFile))
                    .build();

            httppost.setEntity(reqEntity);

            LOG.info("executing request " + httppost.getRequestLine());
            CloseableHttpResponse response = httpclient.execute(httppost);
            try {
                HttpEntity resEntity = response.getEntity();
                LOG.info(IOUtils.toString(resEntity.getContent()));
                EntityUtils.consume(resEntity);
            } finally {
                response.close();
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ConfigurationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
