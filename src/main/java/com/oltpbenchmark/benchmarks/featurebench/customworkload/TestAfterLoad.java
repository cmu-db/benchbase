package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import com.oltpbenchmark.benchmarks.featurebench.utils.PrimaryIntGen;
import com.oltpbenchmark.benchmarks.featurebench.utils.RandomAString;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Arrays;


public class TestAfterLoad extends YBMicroBenchmark {

    public final static Logger LOG = Logger.getLogger(TestAfterLoad.class);

    public TestAfterLoad(HierarchicalConfiguration<ImmutableNode> config) {
        super(config);
        this.loadOnceImplemented = true;
        this.afterLoadImplemented = true;
    }

    @Override
    public void create(Connection conn) throws SQLException {
        try {
            Statement stmtObj = conn.createStatement();
            LOG.info("Recreating tables if already exists");
            stmtObj.execute("DROP TABLE IF EXISTS categories cascade;");
            stmtObj.execute("CREATE TABLE categories(categoryId int NOT NULL, categoryName varchar(100) NOT NULL,PRIMARY KEY (categoryId));");
            stmtObj.execute("DROP TABLE IF EXISTS products;");
            stmtObj.execute("CREATE TABLE products(productId int NOT NULL,productName varchar(100) NOT NULL, PRIMARY KEY (productId));");
            stmtObj.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void loadOnce(Connection conn) throws SQLException {

        PrimaryIntGen pk = new PrimaryIntGen(Arrays.asList(1, 50));
        RandomAString randomAstring = new RandomAString(Arrays.asList(3, 5));
        int batchSize = 10;
        String insertStmt1 = "INSERT INTO categories(categoryId,categoryName) VALUES (?,?);";
        PreparedStatement stmt = conn.prepareStatement(insertStmt1);
        int currentBatchSize = 0;
        for (int i = 0; i < 50; i++) {
            stmt.setObject(1, pk.run());
            try {
                stmt.setObject(2, randomAstring.run());
            } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException |
                     InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            currentBatchSize += 1;
            stmt.addBatch();
            if (currentBatchSize == batchSize) {
                stmt.executeBatch();
                currentBatchSize = 0;
            }
        }
        pk = new PrimaryIntGen(Arrays.asList(1, 50));
        String insertStmt2 = "INSERT INTO products(productId,productName) VALUES (?,?);";
        stmt = conn.prepareStatement(insertStmt2);
        currentBatchSize = 0;
        for (int i = 0; i < 50; i++) {
            stmt.setObject(1, pk.run());
            try {
                stmt.setObject(2, randomAstring.run());
            } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException |
                     InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            currentBatchSize += 1;
            stmt.addBatch();
            if (currentBatchSize == batchSize) {
                stmt.executeBatch();
                currentBatchSize = 0;
            }
        }
        stmt.close();
    }

    @Override
    public void afterLoad(Connection conn) throws SQLException {
        System.out.println("In after load of user");
        Statement stmtObj = conn.createStatement();
        stmtObj.execute("ALTER TABLE products add column categoryId int constraint fk_category_categoryId REFERENCES categories(categoryId);");
        stmtObj.close();
    }
}