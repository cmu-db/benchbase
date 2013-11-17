package com.oltpbenchmark.util;

import com.oltpbenchmark.Results;
import com.oltpbenchmark.catalog.Catalog;
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
import java.sql.*;
import java.util.Map;
import java.util.TreeMap;

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

    public static void uploadResult(Results r, XMLConfiguration conf, XMLConfiguration pluginConfig, CommandLine argsLine) throws ParseException {
        String dbUrl = conf.getString("DBUrl");
        String dbType = conf.getString("dbtype");
        String username = conf.getString("username");
        String password = conf.getString("password");
        String benchType = argsLine.getOptionValue("b");
        int windowSize = Integer.parseInt(argsLine.getOptionValue("s"));
        String uploadCode = conf.getString("uploadCode");
        String uploadUrl = conf.getString("uploadUrl");

        String classname = pluginConfig.getString("/plugin[@name='collector-" + dbType + "']");

        if (classname == null)
        {
            throw new ParseException("Plugin collector-" + dbType + " is undefined in config/plugin.xml");
        }

        DBParameterCollector collector = ClassUtil.newInstance(classname, new Object[] { }, new Class<?>[] { });

        Map<String, String> dbConf = collector.collect(dbUrl, username, password);

        XMLConfiguration expConf = (XMLConfiguration) conf.clone();
        for (String key: IGNORE_CONF) {
            expConf.clearProperty(key);
        }

        try {
            File expConfFile = File.createTempFile("expConf", ".tmp");
            File sampleFile = File.createTempFile("sample", ".tmp");
            File summaryFile = File.createTempFile("summary", ".tmp");
            File dbConfFile = File.createTempFile("dbConf", ".tmp");
            File rawDataFile = File.createTempFile("raw", ".tmp");

            PrintStream confOut = new PrintStream(new FileOutputStream(expConfFile));
            expConf.save(confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(sampleFile));
            r.writeCSV(windowSize, confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(summaryFile));
            confOut.println(dbType);
            confOut.println(benchType);
            confOut.println(r.latencyDistribution.toString());
            confOut.println(r.getRequestsPerSecond());
            confOut.println(expConf.getString("isolation"));
            confOut.println(expConf.getString("scalefactor"));
            confOut.println(expConf.getString("terminals"));
            confOut.close();

            confOut= new PrintStream(new FileOutputStream(dbConfFile));
            for (Map.Entry<String, String> kv: dbConf.entrySet()) {
                confOut.println(kv.getKey() + ":" + kv.getValue());
            }
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(rawDataFile));
            r.writeAllCSVAbsoluteTiming(confOut);
            confOut.close();

            byte[] buf = new byte[1024];
            File uploadFile = File.createTempFile("upload", ".tmp");
            confOut= new PrintStream(new FileOutputStream(uploadFile));
            confOut.println(count(dbConfFile.getAbsolutePath()));
            confOut.println(count(expConfFile.getAbsolutePath()));
            confOut.println(count(summaryFile.getAbsolutePath()));

            InputStream in = new FileInputStream(dbConfFile);
            int b = 0;
            while ( (b = in.read(buf)) >= 0) {
                confOut.write(buf, 0, b);
            }
            in.close();

            in = new FileInputStream(expConfFile);
            while ( (b = in.read(buf)) >= 0) {
                confOut.write(buf, 0, b);
            }
            in.close();

            in = new FileInputStream(summaryFile);
            while ( (b = in.read(buf)) >= 0) {
                confOut.write(buf, 0, b);
            }
            in.close();

            confOut.close();

            CloseableHttpClient httpclient = HttpClients.createDefault();
            HttpPost httppost = new HttpPost(uploadUrl);
            FileBody bin = new FileBody(uploadFile);

            HttpEntity reqEntity = MultipartEntityBuilder.create()
                    .addTextBody("upload_code", uploadCode)
                    .addPart("data", bin)
                    .addPart("raw_data", new FileBody(rawDataFile))
                    .addPart("sample_data", new FileBody(sampleFile))
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

    static protected int count(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }
}

interface DBParameterCollector {
    Map<String, String> collect(String oriDBUrl, String username, String password);
}

class MYSQLCollector implements DBParameterCollector {
    private static final Logger LOG = Logger.getLogger(MYSQLCollector.class);

    public MYSQLCollector() {
    }

    @Override
    public Map<String, String> collect(String oriDBUrl, String username, String password) {
        Map<String, String> results = new TreeMap<String, String>();
        String dbUrl = oriDBUrl.substring(0, oriDBUrl.lastIndexOf('/'));
        dbUrl = dbUrl + "/information_schema";
        try {
            Connection conn = DriverManager.getConnection(dbUrl, username, password);
            Catalog.setSeparator(conn);
            Statement s = conn.createStatement();
            ResultSet out = s.executeQuery("SELECT * FROM GLOBAL_VARIABLES;");
            while(out.next()) {
                results.put(out.getString("VARIABLE_NAME"), out.getString("VARIABLE_VALUE"));
            }
        } catch (SQLException e) {
            LOG.debug("Error while collecting DB parameters: " + e.getMessage());
        }
        return results;
    }
}

class POSTGRESCollector implements DBParameterCollector {
    private static final Logger LOG = Logger.getLogger(POSTGRESCollector.class);

    public POSTGRESCollector() {
    }

    @Override
    public Map<String, String> collect(String oriDBUrl, String username, String password) {
        Map<String, String> results = new TreeMap<String, String>();
        try {
            Connection conn = DriverManager.getConnection(oriDBUrl, username, password);
            Catalog.setSeparator(conn);
            Statement s = conn.createStatement();
            ResultSet out = s.executeQuery("SHOW ALL;");
            while(out.next()) {
                results.put(out.getString("name"), out.getString("setting"));
                System.out.println(out.getString("name") + ":" + out.getString("setting"));
            }
        } catch (SQLException e) {
            LOG.debug("Error while collecting DB parameters: " + e.getMessage());
        }
        return results;
    }
}
