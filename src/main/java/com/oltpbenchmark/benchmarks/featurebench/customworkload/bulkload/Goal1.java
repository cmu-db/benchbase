package com.oltpbenchmark.benchmarks.featurebench.customworkload.bulkload;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import com.oltpbenchmark.benchmarks.featurebench.customworkload.bulkload.utils.BulkloadUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class Goal1 extends YBMicroBenchmark {
    private static final Logger LOG = LoggerFactory.getLogger(Goal1.class);
    String tableName;
    int numOfColumns;
    int numOfRows;
    int indexCount;
    String filePath;
    int stringLength;

    boolean recreateCsvIfExists;

    public Goal1(HierarchicalConfiguration<ImmutableNode> config) {
        super(config);
        this.executeOnceImplemented = true;
        this.loadOnceImplemented = true;
        this.tableName = config.getString("/tableName");
        this.numOfColumns = config.getInt("/columns");
        this.numOfRows = config.getInt("/rows");
        this.indexCount = config.getInt("/indexes");
        this.filePath = config.getString("/filePath");
        this.stringLength = config.getInt("/stringLength");
        this.recreateCsvIfExists = config.getBoolean("/recreateCsvIfExists", true);
    }

    public void create(Connection conn) throws SQLException {
        Statement stmtOBj = conn.createStatement();
        LOG.info("Recreate table if it exists");
        stmtOBj.executeUpdate(String.format("DROP TABLE IF EXISTS %s", this.tableName));
        stmtOBj.close();
        createTableAndIndexes(conn);

        if(!this.recreateCsvIfExists && (new File(this.filePath).exists())) {
            LOG.info("Using existing CSV file.");
        }
        else {
            LOG.info("Create CSV file with data");
            BulkloadUtils.createCSV(this.filePath, this.numOfRows, this.numOfColumns, this.stringLength);
        }
    }

    public void loadOnce(Connection conn) throws SQLException {
    }

    public void executeOnce(Connection conn, BenchmarkModule benchmarkModule) throws SQLException {
        BulkloadUtils.runCopyCommand(conn, this.tableName, this.filePath);
    }

    public void createTableAndIndexes(Connection conn) {

        BulkloadUtils.createTable(conn, this.tableName, this.numOfColumns);
        BulkloadUtils.createIndexes(conn, this.indexCount, this.tableName);

    }


    @Override
    public void cleanUp(Connection conn) throws SQLException {
        BulkloadUtils.cleanUp(conn, this.tableName);
    }
}
