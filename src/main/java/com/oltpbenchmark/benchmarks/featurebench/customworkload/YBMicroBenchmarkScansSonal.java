package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import com.oltpbenchmark.benchmarks.featurebench.helpers.ExecuteRule;
import com.oltpbenchmark.benchmarks.featurebench.helpers.LoadRule;

import com.oltpbenchmark.types.State;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;


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

    @Override
    public ArrayList<LoadRule> loadRules() {
        return null;
    }

    @Override
    public ArrayList<ExecuteRule> executeRules() {
        return null;
    }

    public void loadOnce(Connection conn) throws SQLException{

        String insertStmt = "INSERT INTO demoScans SELECT random() * 1000,  generate_series(1, 1000);";
        Statement stmtOBj = conn.createStatement();
        stmtOBj.execute(insertStmt);

    }
}
