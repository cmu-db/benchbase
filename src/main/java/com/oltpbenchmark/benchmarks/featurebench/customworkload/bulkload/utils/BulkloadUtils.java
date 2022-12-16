package com.oltpbenchmark.benchmarks.featurebench.customworkload.bulkload.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
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
import java.util.ArrayList;
import java.util.List;

public class BulkloadUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BulkloadUtils.class);

    public static void createCSV(String filePath, int numOfRows, int numOfColumns, int stringLength) {
        try {
            FileOutputStream outputStream = new FileOutputStream(filePath);

            for (int i = 0; i < numOfRows; i++) {
                StringBuilder row = new StringBuilder();
                row.append(i);
                for (int j = 0; j < numOfColumns; j++) {
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

    public static void createTable(Connection conn, String tableName, int numOfColumns) {
        StringBuilder createStmt = new StringBuilder();
        createStmt.append(String.format("CREATE TABLE %s (id INT, ", tableName));
        for (int i = 1; i <= numOfColumns; i++) {
            createStmt.append(String.format("col%d TEXT ,", i));
        }
        try {
            if (conn.getMetaData().getUserName().equalsIgnoreCase("yugabyte")) {
                createStmt.append("primary key (id asc) );");
            } else {
                createStmt.append("primary key (id) );");
            }

            Statement stmt = conn.createStatement();
            stmt.execute(createStmt.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createIndexes(Connection conn, int indexCount, String tableName) {
        List<String> ddls = new ArrayList<>();
        for (int i = 0; i < indexCount; i++) {
            ddls.add(String.format("CREATE INDEX idx%d on %s(col%d);", i + 1, tableName, i + 1));
        }
        ddls.forEach(ddl -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void runCopyCommand(Connection conn, String tableName, String filePath) {
        try {
            String copyCommand = String.format(
                "COPY %s FROM STDIN (FORMAT CSV, HEADER false)",
                tableName);
            long rowsInserted = 0;
            if (conn.getMetaData().getUserName().equalsIgnoreCase("yugabyte")) {
                rowsInserted = new com.yugabyte.copy.CopyManager((com.yugabyte.core.BaseConnection) conn)
                    .copyIn(copyCommand,
                        new BufferedReader(new FileReader(filePath)));
            } else {
                rowsInserted = new CopyManager((BaseConnection) conn)
                    .copyIn(copyCommand,
                        new BufferedReader(new FileReader(filePath)));
            }
            LOG.info("Number of rows Inserted: {}", rowsInserted);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void cleanUp(Connection conn,String tableName) throws SQLException {
        String countQuery = "select count(*) from " + tableName;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(countQuery);
        long count = 0;
        while (rs.next()) {
            count = rs.getLong(1);
        }
        LOG.info("Number of rows inserted are: " + count);
    }

}
