package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import com.yugabyte.copy.CopyManager;
import com.yugabyte.core.BaseConnection;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class CopyCommand extends YBMicroBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(CopyCommand.class);

    public CopyCommand(HierarchicalConfiguration<ImmutableNode> config) {
        super(config);
        this.executeOnceImplemented = true;
        this.loadOnceImplemented = true;
    }

    public void create(Connection conn) throws SQLException {
        String tableName = config.getString("/tableName");
        int numOfColumns = config.getInt("/columns");
        int numOfRows = config.getInt("/rows");
        int indexCount = config.getInt("/indexes");
        String filePath = config.getString("/filePath");

        Statement stmtOBj = conn.createStatement();
        LOG.info("Recreate table if it exists");
        stmtOBj.executeUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        createTable(conn, tableName, numOfColumns, indexCount);
        LOG.info("Create CSV file with data");
        createCSV(numOfColumns, numOfRows, filePath);
        stmtOBj.close();
    }

    public void cleanUp(Connection conn) throws SQLException {
        String tableName = config.getString("/tableName");
        Statement stmtOBj = conn.createStatement();
        LOG.info("=======DROP ALL THE TABLES=======");
        stmtOBj.executeUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        stmtOBj.close();
    }

    public void loadOnce(Connection conn) throws SQLException{
    }

    public void executeOnce(Connection conn) throws SQLException {
        String tableName = config.getString("/tableName");
        int rowsPerTransaction = config.getInt("/rowsPerTransaction");
        String filePath = config.getString("/filePath");
        runCopyCommand(conn, tableName, filePath, rowsPerTransaction);
    }

    public void createTable(Connection conn, String tableName, int numberOfColums, int indexCount) {
        List<String> ddls = new ArrayList<>();
        StringBuilder createStmt = new StringBuilder();
        createStmt.append(String.format("CREATE TABLE %s (id INT primary key, ", tableName));
        for (int i = 1; i <= numberOfColums; i++) {
            createStmt.append(String.format("col%d TEXT", i));
            if (i != numberOfColums)
                createStmt.append(",");
            else
                createStmt.append(");");
        }
        ddls.add(createStmt.toString());

        if (indexCount > 0 && indexCount <= numberOfColums) {
            for (int i = 0; i < indexCount; i++) {
                ddls.add(String.format("CREATE INDEX idx%d on %s(col%d);", i + 1, tableName, i + 1));
            }
        }

        ddls.forEach(ddl -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void createCSV(int numberOfColums, int numberOfRows, String filePath) {
        try {
            FileOutputStream outputStream = new FileOutputStream(filePath);
            int stringLength = 16;
            for (int i = 0; i < numberOfRows; i++) {
                StringBuilder row = new StringBuilder();
                row.append(i);
                for (int j = 0; j < numberOfColums; j++) {
                    row.append(", ");
                    row.append(RandomStringUtils.random(stringLength, true, false));
                }
                row.append("\n");
                outputStream.write(row.toString().getBytes());
            }
            outputStream.close();
            LOG.info("CSV file {} created", filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void runCopyCommand(Connection conn, String tableName, String filePath, int rowsPerTransaction) {
        try {
            String copyCommand = String.format(
                "COPY %s FROM STDIN (FORMAT CSV, HEADER false, ROWS_PER_TRANSACTION %d)",
                tableName, rowsPerTransaction
            );
            long rowsInserted = new CopyManager((BaseConnection) conn)
                .copyIn(copyCommand,
                    new BufferedReader(new FileReader(filePath)));
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }


}
