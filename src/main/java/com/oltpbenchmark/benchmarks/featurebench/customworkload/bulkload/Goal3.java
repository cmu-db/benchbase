package com.oltpbenchmark.benchmarks.featurebench.customworkload.bulkload;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import com.oltpbenchmark.benchmarks.featurebench.customworkload.bulkload.utils.BulkloadUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Goal3 extends YBMicroBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(Goal3.class);

    String tableName;
    int numOfColumns;
    int numOfRows;
    String filePath;
    int stringLength;

    public Goal3(HierarchicalConfiguration<ImmutableNode> config) {
        super(config);
        this.executeOnceImplemented = true;
        this.loadOnceImplemented = true;
        this.tableName = config.getString("/tableName");
        this.numOfColumns = config.getInt("/columns");
        this.numOfRows = config.getInt("/rows");
        this.filePath = config.getString("/filePath");
        this.stringLength = config.getInt("/stringLength");
    }

    public void create(Connection conn) throws SQLException {
        Statement stmtOBj = conn.createStatement();
        LOG.info("Recreate table if it exists");
        stmtOBj.executeUpdate(String.format("DROP TABLE IF EXISTS %s", this.tableName));
        stmtOBj.close();
        LOG.info("Creating table");
        BulkloadUtils.createTable(conn, this.tableName, this.numOfColumns);
        LOG.info("Create CSV file with data");
        BulkloadUtils.createCSV(this.filePath, this.numOfRows, this.numOfColumns, this.stringLength);

    }

    public void loadOnce(Connection conn) throws SQLException {
    }

    public void executeOnce(Connection conn, BenchmarkModule benchmarkModule) throws SQLException {
        BulkloadUtils.runCopyCommand(conn, this.tableName, this.filePath);
    }

    @Override
    public void cleanUp(Connection conn) throws SQLException {
        BulkloadUtils.cleanUp(conn, this.tableName);
    }

}
