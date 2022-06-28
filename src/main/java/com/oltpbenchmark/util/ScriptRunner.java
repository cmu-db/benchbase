/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/*
 * Slightly modified version of the com.ibatis.common.jdbc.ScriptRunner class
 * from the iBATIS Apache project. Only removed dependency on Resource class
 * and a constructor
 */
package com.oltpbenchmark.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;

/**
 * Tool to run database scripts
 * http://pastebin.com/f10584951
 */
public class ScriptRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ScriptRunner.class);

    private static final String DEFAULT_DELIMITER = ";";

    private final Connection connection;
    private final boolean stopOnError;
    private final boolean autoCommit;

    /**
     * Default constructor
     */
    public ScriptRunner(Connection connection, boolean autoCommit, boolean stopOnError) {
        this.connection = connection;
        this.autoCommit = autoCommit;
        this.stopOnError = stopOnError;
    }


    public void runExternalScript(String path) throws IOException, SQLException {

        LOG.debug("trying to find external file by path {}", path);

        try (FileReader reader = new FileReader(path)) {

            runScript(reader);
        }
    }

    public void runScript(String path) throws IOException, SQLException {

        LOG.debug("trying to find file by path {}", path);

        try (InputStream in = this.getClass().getResourceAsStream(path);
             Reader reader = new InputStreamReader(in)) {

            runScript(reader);
        }
    }

    private void runScript(Reader reader) throws IOException, SQLException {
        boolean originalAutoCommit = connection.getAutoCommit();

        try {
            if (originalAutoCommit != this.autoCommit) {
                connection.setAutoCommit(this.autoCommit);
            }
            runScript(connection, reader);
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }


    /**
     * Runs an SQL script (read in using the Reader parameter) using the
     * connection passed in
     *
     * @param conn   - the connection to use for the script
     * @param reader - the source of the script
     * @throws SQLException if any SQL errors occur
     * @throws IOException  if there is an error reading from the Reader
     */
    private void runScript(Connection conn, Reader reader) throws IOException, SQLException {
        StringBuffer command = null;
        try (LineNumberReader lineReader = new LineNumberReader(reader)) {
            String line = null;
            while ((line = lineReader.readLine()) != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(line);
                }
                if (command == null) {
                    command = new StringBuffer();
                }
                String trimmedLine = line.trim();
                line = line.replaceAll("\\-\\-.*$", ""); // remove comments in line;

                if (trimmedLine.startsWith("--") || trimmedLine.startsWith("//")) {
                    LOG.debug(trimmedLine);
                } else if (trimmedLine.length() < 1) {
                    // Do nothing
                } else if (trimmedLine.endsWith(getDelimiter())) {
                    command.append(line, 0, line.lastIndexOf(getDelimiter()));
                    command.append(" ");

                    try (Statement statement = conn.createStatement()) {

                        boolean hasResults = false;
                        final String sql = command.toString().trim();
                        if (stopOnError) {
                            hasResults = statement.execute(sql);
                        } else {
                            try {
                                statement.execute(sql);
                            } catch (SQLException e) {
                                LOG.error(e.getMessage(), e);
                            }
                        }

                        if (autoCommit && !conn.getAutoCommit()) {
                            conn.commit();
                        }

                        // HACK
                        if (hasResults && !sql.toUpperCase().startsWith("CREATE")) {
                            try (ResultSet rs = statement.getResultSet()) {
                                if (hasResults && rs != null) {
                                    ResultSetMetaData md = rs.getMetaData();
                                    int cols = md.getColumnCount();
                                    for (int i = 0; i < cols; i++) {
                                        String name = md.getColumnLabel(i);
                                        LOG.debug(name);
                                    }

                                    while (rs.next()) {
                                        for (int i = 0; i < cols; i++) {
                                            String value = rs.getString(i);
                                            LOG.debug(value);
                                        }
                                    }

                                }
                            }
                        }

                        command = null;
                    } finally {

                        Thread.yield();
                    }
                } else {
                    command.append(line);
                    command.append(" ");
                }
            }
            if (!autoCommit) {
                conn.commit();
            }
        } finally {
            if (!autoCommit) {
                conn.rollback();
            }
        }
    }

    private String getDelimiter() {
        return DEFAULT_DELIMITER;
    }

}
