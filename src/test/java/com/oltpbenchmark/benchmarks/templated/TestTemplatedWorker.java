package com.oltpbenchmark.benchmarks.templated;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.oltpbenchmark.DBWorkload;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTemplatedWorker extends AbstractTestWorker<TemplatedBenchmark> {
  private static final Logger LOG = LoggerFactory.getLogger(TestTemplatedWorker.class);

  public static final String DDL_OVERRIDE_PATH =
      Paths.get("src", "main", "resources", "benchmarks", "tpcc", "ddl-generic.sql")
          .toAbsolutePath()
          .toString();
  public static final String SAMPLE_TEMPLATED_LOADING_CONFIG =
      Paths.get("config", "sqlite", "sample_tpcc_config.xml").toAbsolutePath().toString();
  public static final String SAMPLE_TEMPLATED_CONFIG =
      Paths.get("config", "sqlite", "sample_templated_config.xml").toAbsolutePath().toString();
  public static final String TEMPLATES_CONFIG =
      Paths.get("data", "templated", "example.xml").toAbsolutePath().toString();

  TPCCBenchmark tpccBenchmark = null;

  public TestTemplatedWorker() {
    // Technically we aren't creating this schema with the
    // TemplatedBenchmark, but specifying the DDL that we are using (see
    // below) allows some other checks to pass.
    super(DDL_OVERRIDE_PATH);
  }

  public static void setWorkloadConfigXml(WorkloadConfiguration workConf) {
    // Load the configuration file so we can parse the query_template_file value.
    try {
      XMLConfiguration xmlConf = DBWorkload.buildConfiguration(SAMPLE_TEMPLATED_CONFIG);
      workConf.setXmlConfig(xmlConf);
    } catch (ConfigurationException ex) {
      LOG.error("Error loading configuration: " + SAMPLE_TEMPLATED_CONFIG, ex);
    }
  }

  @Override
  protected void customWorkloadConfiguration(WorkloadConfiguration workConf) {
    setWorkloadConfigXml(workConf);
  }

  @Override
  public List<Class<? extends Procedure>> procedures() {
    // Note: the first time this is called is before the benchmark is
    // initialized, so it should return nothing.
    // It's only populated after the config is loaded for the benchmark.
    List<Class<? extends Procedure>> procedures = new ArrayList<>();
    if (this.benchmark != null) {
      procedures = this.benchmark.getProcedureClasses();
      if (!procedures.isEmpty() && this.workConf.getTransTypes().isEmpty()) {
        workConf.setTransTypes(proceduresToTransactionTypes(procedures));
      }
    }
    return procedures;
  }

  @Override
  public Class<TemplatedBenchmark> benchmarkClass() {
    return TemplatedBenchmark.class;
  }

  private void setupTpccBenchmarkHelper() throws SQLException {
    if (this.tpccBenchmark != null) {
      return;
    }

    // Create a second benchmark to re/ab/use for loading the database (tpcc in this case).
    WorkloadConfiguration tpccWorkConf = new WorkloadConfiguration();
    tpccWorkConf.setDatabaseType(this.workConf.getDatabaseType());
    tpccWorkConf.setUrl(this.workConf.getUrl());
    tpccWorkConf.setScaleFactor(this.workConf.getScaleFactor());
    tpccWorkConf.setTerminals(this.workConf.getTerminals());
    tpccWorkConf.setBatchSize(this.workConf.getBatchSize());
    // tpccWorkConf.setBenchmarkName(BenchmarkModule.convertBenchmarkClassToBenchmarkName(TPCCBenchmark.class));
    tpccWorkConf.setBenchmarkName(
        TPCCBenchmark.class.getSimpleName().toLowerCase().replace("benchmark", ""));

    this.tpccBenchmark = new TPCCBenchmark(this.workConf);
    conn = this.tpccBenchmark.makeConnection();
    assertNotNull(conn);
    this.tpccBenchmark.refreshCatalog();
    catalog = this.tpccBenchmark.getCatalog();
    assertNotNull(catalog);
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
