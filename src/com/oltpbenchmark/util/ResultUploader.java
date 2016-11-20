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
import com.oltpbenchmark.util.dbms_collectors.DBParameterCollector;
import com.oltpbenchmark.util.dbms_collectors.DBParameterCollectorGen;
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
import java.util.Map;
import java.util.TimeZone;
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
    DBParameterCollector collector;

    String dbUrl, dbType;
    String username, password;
    String benchType;
    int windowSize;
    String uploadCode, uploadUrl;

    public ResultUploader(Results r, XMLConfiguration conf, CommandLine argsLine) {
        this.expConf = conf;
        this.results = r;
        this.argsLine = argsLine;

        dbUrl = expConf.getString("DBUrl");
        dbType = expConf.getString("dbtype");
        username = expConf.getString("username");
        password = expConf.getString("password");
        benchType = argsLine.getOptionValue("b");
        windowSize = Integer.parseInt(argsLine.getOptionValue("s"));
        uploadCode = expConf.getString("uploadCode");
        uploadUrl = expConf.getString("uploadUrl");

        this.collector = DBParameterCollectorGen.getCollector(dbType, dbUrl, username, password);
    }

    public void writeDBParameters(PrintStream os) {
        String dbConf = collector.collectParameters();
        os.print(dbConf);
    }

    public void writeBenchmarkConf(PrintStream os) throws ConfigurationException {
        XMLConfiguration outputConf = (XMLConfiguration) expConf.clone();
        for (String key: IGNORE_CONF) {
            outputConf.clearProperty(key);
        }
        outputConf.save(os);
    }

    public void writeSummary(PrintStream os) {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Date now = new Date();
        os.println(now.getTime() / 1000L);
        os.println(dbType);
        os.println(collector.collectVersion());
        os.println(benchType);
        os.println(results.latencyDistribution.toString());
        os.println(results.getRequestsPerSecond());
        for (String field: BENCHMARK_KEY_FIELD) {
            os.println(field + "=" + expConf.getString(field));
        }
    }

    public void uploadResult() throws ParseException {
        try {
            File expConfFile = File.createTempFile("expConf", ".tmp");
            File sampleFile = File.createTempFile("sample", ".tmp");
            File summaryFile = File.createTempFile("summary", ".tmp");
            File dbConfFile = File.createTempFile("dbConf", ".tmp");
            File rawDataFile = File.createTempFile("raw", ".gz");

            PrintStream confOut = new PrintStream(new FileOutputStream(expConfFile));
            writeBenchmarkConf(confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(dbConfFile));
            writeDBParameters(confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(sampleFile));
            results.writeCSV(windowSize, confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(summaryFile));
            writeSummary(confOut);
            confOut.close();

            confOut = new PrintStream(new GZIPOutputStream(new FileOutputStream(rawDataFile)));
            results.writeAllCSVAbsoluteTiming(confOut);
            confOut.close();

            CloseableHttpClient httpclient = HttpClients.createDefault();
            HttpPost httppost = new HttpPost(uploadUrl);

            HttpEntity reqEntity = MultipartEntityBuilder.create()
                    .addTextBody("upload_code", uploadCode)
                    .addPart("sample_data", new FileBody(sampleFile))
                    .addPart("raw_data", new FileBody(rawDataFile))
                    .addPart("db_conf_data", new FileBody(dbConfFile))
                    .addPart("benchmark_conf_data", new FileBody(expConfFile))
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
