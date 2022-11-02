package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


public class YBMicroBenchmarkScansSonal extends YBMicroBenchmark {
    public final static Logger LOG = Logger.getLogger(com.oltpbenchmark.benchmarks.featurebench.customworkload.YBMicroBenchmarkImplSonal.class);

    public YBMicroBenchmarkScansSonal(HierarchicalConfiguration<ImmutableNode> config) {
        super(config);
        this.loadOnceImplemented = true;
    }

    @Override
    public void create(Connection conn) throws SQLException {
        try {
            Statement stmtOBj = conn.createStatement();
            LOG.info("Recreating tables if already exists");
            stmtOBj.executeUpdate("DROP TABLE IF EXISTS demoScans;");
            stmtOBj.execute("CREATE TABLE demoScans(num numeric,id int);");
            stmtOBj.execute("CREATE INDEX demoid ON demoScans(num);");
            stmtOBj.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void loadOnce(Connection conn) throws SQLException{

        String insertStmt = "INSERT INTO demoScans SELECT random() * 1000,  generate_series(1, 1000);";
        Statement stmtOBj = conn.createStatement();
        stmtOBj.execute(insertStmt);

    }
    public void executeOnce(Connection conn)throws SQLException{
        String selectStmt = "SELECT * from demoScans;";
        Statement stmtOBj = conn.createStatement();
        stmtOBj.execute(selectStmt);

    }

}
