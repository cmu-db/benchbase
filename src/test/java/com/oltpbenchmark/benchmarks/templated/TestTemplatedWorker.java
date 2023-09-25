package com.oltpbenchmark.benchmarks.templated;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.WorkloadConfiguration;

import com.oltpbenchmark.benchmarks.templated.procedures.*;
import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;

import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;


public class TestTemplatedWorker extends AbstractTestWorker<TemplatedBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(TestTemplatedWorker.class);

    public static final String DDL_OVERRIDE_PATH = Paths.get("src", "main", "resources", "benchmarks", "tpcc", "ddl-generic.sql").toAbsolutePath().toString();
    public static final String SAMPLE_TEMPLATED_LOADING_CONFIG = Paths.get("config", "sqlite", "sample_tpcc_config.xml").toAbsolutePath().toString();
    public static final String SAMPLE_TEMPLATED_CONFIG = Paths.get("config", "sqlite", "sample_templated_config.xml").toAbsolutePath().toString();
    public static final String TEMPLATES_CONFIG = Paths.get("data", "templated", "example.xml").toAbsolutePath().toString();

    TPCCBenchmark tpccBenchmark = null;

    public TestTemplatedWorker() {
        super(DDL_OVERRIDE_PATH);
    }

    public static void setWorkloadConfigXml(WorkloadConfiguration workConf) {
        try {
            XMLConfiguration xmlConf = DBWorkload.buildConfiguration(SAMPLE_TEMPLATED_CONFIG);
            workConf.setXmlConfig(xmlConf);
        }
        catch (ConfigurationException ex) {
            LOG.error("Error loading configuration: " + SAMPLE_TEMPLATED_CONFIG, ex);
        }
    }

    @Override
    protected void customWorkloadConfiguration(WorkloadConfiguration workConf) {
        setWorkloadConfigXml(workConf);
    }

    public static final List<Class<? extends Procedure>> PROCEDURE_CLASSES = List.of(
            GenericQuery.class
    );

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return PROCEDURE_CLASSES;
    }

    @Override
    public Class<TemplatedBenchmark> benchmarkClass() {
        return TemplatedBenchmark.class;
    }

    private void setupTpccBenchmarkHelper() throws SQLException {
        if (this.tpccBenchmark != null) {
            return;
        }

        WorkloadConfiguration tpccWorkConf = new WorkloadConfiguration();
        tpccWorkConf.setDatabaseType(this.workConf.getDatabaseType());
        tpccWorkConf.setUrl(this.workConf.getUrl());
        tpccWorkConf.setScaleFactor(this.workConf.getScaleFactor());
        tpccWorkConf.setTerminals(this.workConf.getTerminals());
        tpccWorkConf.setBatchSize(this.workConf.getBatchSize());
        // tpccWorkConf.setBenchmarkName(BenchmarkModule.convertBenchmarkClassToBenchmarkName(TPCCBenchmark.class));
        tpccWorkConf.setBenchmarkName(TPCCBenchmark.class.getSimpleName().toLowerCase().replace("benchmark", ""));

        this.tpccBenchmark = new TPCCBenchmark(this.workConf);
        conn = this.tpccBenchmark.makeConnection();
        assertNotNull(conn);
        this.tpccBenchmark.refreshCatalog();
        catalog = this.tpccBenchmark.getCatalog();
    }

    protected void createDatabase() {
        try {
            this.setupTpccBenchmarkHelper();
            this.tpccBenchmark.createDatabase();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            cleanupServer();
            fail("createDatabase() failed");
        }
    }

    protected void loadDatabase() {
        try {
            this.setupTpccBenchmarkHelper();
            this.tpccBenchmark.loadDatabase();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            cleanupServer();
            fail("loadDatabase() failed");
        }
    }
}
