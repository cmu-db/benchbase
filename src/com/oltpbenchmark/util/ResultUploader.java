package com.oltpbenchmark.util;

import com.oltpbenchmark.Results;
import com.oltpbenchmark.catalog.Catalog;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
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

        LOG.info("Uploading results");

        XMLConfiguration expConf = (XMLConfiguration) conf.clone();
        for (String key: IGNORE_CONF) {
            expConf.clearProperty(key);
        }

        try {
            File expConfFile = File.createTempFile("expConf", ".tmp");
            File sampleFile = File.createTempFile("sample", ".tmp");
            File summaryFile = File.createTempFile("summary", ".tmp");
            File dbConfFile = File.createTempFile("dbConf", ".tmp");

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

            Process proc = Runtime.getRuntime().exec(new String[]{
                    "tools/upload.sh",
                    expConfFile.getAbsolutePath(),
                    sampleFile.getAbsolutePath(),
                    summaryFile.getAbsolutePath(),
                    dbConfFile.getAbsolutePath(),
                    uploadCode,
                    uploadUrl,
            });

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String s;
            while ((s = stdInput.readLine()) != null) {
                LOG.info(s);
            }

            proc.waitFor();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ConfigurationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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
        String dbUrl = oriDBUrl;
        try {
            Connection conn = DriverManager.getConnection(dbUrl, username, password);
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
