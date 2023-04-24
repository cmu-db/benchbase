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

public class Goal2 extends YBMicroBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(Goal2.class);

    String tableName;
    int numOfColumns;
    int numOfRows;
    int indexCount;
    String filePath;
    int stringLength;

    boolean create_index_before_load;
    boolean create_index_after_load;

    boolean recreateCsvIfExists;

    public Goal2(HierarchicalConfiguration<ImmutableNode> config) {
        super(config);
        this.executeOnceImplemented = true;
        this.loadOnceImplemented = true;
        this.tableName = config.getString("/tableName");
        this.numOfColumns = config.getInt("/columns");
        this.numOfRows = config.getInt("/rows");
        this.indexCount = config.getInt("/indexes");
        this.filePath = config.getString("/filePath");
        this.stringLength = config.getInt("/stringLength");
        this.create_index_before_load = config.getBoolean("/create_index_before_load");
        this.create_index_after_load = config.getBoolean("/create_index_after_load");
        this.recreateCsvIfExists = config.getBoolean("/recreateCsvIfExists", true);
    }

    public void create(Connection conn) throws SQLException {
        Statement stmtOBj = conn.createStatement();
        LOG.info("Recreate table if it exists");
        stmtOBj.executeUpdate(String.format("DROP TABLE IF EXISTS %s", this.tableName));
        stmtOBj.close();
        LOG.info("Creating table");
        BulkloadUtils.createTable(conn, this.tableName, this.numOfColumns);
        if(!this.recreateCsvIfExists && (new File(this.filePath).exists()))
            LOG.info("Using existing CSV file.");
        else {
            LOG.info("Create CSV file with data");
            BulkloadUtils.createCSV(this.filePath, this.numOfRows, this.numOfColumns, this.stringLength);
        }
    }

    public void loadOnce(Connection conn) throws SQLException {
    }

    public void executeOnce(Connection conn, BenchmarkModule benchmarkModule) throws SQLException {
        if (this.create_index_before_load && (this.indexCount > 0 && this.indexCount <= this.numOfColumns)) {
            LOG.info("Creating indexes before load");
            BulkloadUtils.createIndexes(conn, this.indexCount, this.tableName);
//            if(!conn.getAutoCommit()) conn.commit();
            LOG.info("Done creating indexes");
        }
        BulkloadUtils.runCopyCommand(conn, this.tableName, this.filePath);
//        if(!conn.getAutoCommit()) conn.commit();
        if (this.create_index_after_load && (this.indexCount > 0 && this.indexCount <= this.numOfColumns)) {
            LOG.info("Creating indexes after load(index back-filling)");
            BulkloadUtils.createIndexes(conn, this.indexCount, this.tableName);
            LOG.info("Done creating indexes(Index back-filling done)");
        }
    }


    @Override
    public void cleanUp(Connection conn) throws SQLException {
        BulkloadUtils.cleanUp(conn, this.tableName);
    }
}
