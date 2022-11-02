package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import com.oltpbenchmark.benchmarks.featurebench.utils.OneStringFromArray;
import com.oltpbenchmark.benchmarks.featurebench.utils.RandomAString;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class YBMicroBenchmarkImplSonal extends YBMicroBenchmark {

    public final static Logger LOG = Logger.getLogger(YBMicroBenchmarkImplSonal.class);

    public YBMicroBenchmarkImplSonal(HierarchicalConfiguration<ImmutableNode> config) {
        super(config);
        this.loadOnceImplemented = true;
    }

    @Override
    public void create(Connection conn) throws SQLException {
        try {
            Statement stmtOBj = conn.createStatement();
            LOG.info("Recreating tables if already exists");
            stmtOBj.executeUpdate("DROP TABLE IF EXISTS distributors;");
            stmtOBj.execute("DROP SEQUENCE IF EXISTS serial_no;");
            stmtOBj.execute("CREATE SEQUENCE serial_no increment 1 start 1;");
            stmtOBj.execute("CREATE TABLE distributors(did DECIMAL(5) DEFAULT NEXTVAL('serial_no'), " +
                "dname varchar(40) DEFAULT 'Lusofilms');");

            stmtOBj.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    public void loadOnce(Connection conn) throws SQLException {

        RandomAString randomAstring = new RandomAString(Arrays.asList(1, 20));
        List<String> vals = new ArrayList<>(Arrays.asList("luso films", "Associated Computing, Inc", "XYZ Widgets", "Gizmo Transglobal", "Redline GmbH", "Acme Corporation"));
        int batchSize = 100;
        Random random = new Random();
        String insertStmt = "INSERT INTO distributors (dname) VALUES (?);";
        PreparedStatement stmt = conn.prepareStatement(insertStmt);
        int currentBatchSize = 0;
        for (int i = 0; i < 500; i++) {
            stmt.setObject(1, vals.get(random.nextInt(vals.size())));
            currentBatchSize += 1;
            stmt.addBatch();
            if (currentBatchSize == batchSize) {
                stmt.executeBatch();
                currentBatchSize = 0;
            }
        }
        for (int i = 0; i < 500; i++) {
            try {
                stmt.setObject(1, randomAstring.run());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            currentBatchSize += 1;
            stmt.addBatch();
            if (currentBatchSize == batchSize) {
                stmt.executeBatch();
                currentBatchSize = 0;
            }
        }
        if (currentBatchSize != 0) {
            stmt.executeBatch();
        }
    }

    public void execute(Connection conn) throws SQLException {

        String insertStmt = "INSERT INTO distributors (dname) VALUES (?);";
        PreparedStatement stmt = conn.prepareStatement(insertStmt);
        // Random String insertion
        RandomAString randomAstring = new RandomAString(Arrays.asList(1, 20));
        for (int i = 1; i <= 20; i++) {
            try {
                stmt.setObject(1, randomAstring.run());
                stmt.addBatch();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        stmt.executeBatch();
        // Name in enum insertion
        OneStringFromArray str = new OneStringFromArray(Arrays.asList("luso films", "Associated Computing, Inc", "XYZ Widgets", "Gizmo Transglobal", "Redline GmbH", "Acme Corporation"));
        for (int i = 1; i <= 20; i++) {
            try {
                stmt.setObject(1, str.run());
                stmt.addBatch();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        stmt.executeBatch();
    }

/*    public void cleanUp(Connection conn) throws SQLException {
        try {
            Statement stmtOBj = conn.createStatement();
            LOG.info("\n=======DROP ALL THE TABLES=======");
            stmtOBj.executeUpdate("DROP TABLE distributors");
            LOG.info("\n=======TABLES ARE SUCCESSFULLY DROPPED FROM THE DATABASE=======\n");
            stmtOBj.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }*/
}


