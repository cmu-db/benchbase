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

package com.oltpbenchmark.benchmarks.sleep.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * The actual Sleep implementation
 *
 * @author pavlo
 * @author eric-haibin-lin
 */
public class Sleep extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(Sleep.class);


    // The query only contains a semi-colon
    // That is enough for the DBMS to have to parse it and do something
    public final SQLStmt sleepStmt = new SQLStmt(";");

    public void run(Connection conn, double querySleepTime, double sleepBetweenQueries, int sleepsPerTxn) {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, sleepStmt)) {
            stmt.setDouble(1, querySleepTime);

            for (int i = 0; i < sleepsPerTxn; i++) {
                if (stmt.execute()) {
                    ResultSet r = stmt.getResultSet();
                    while (r.next()) {
                        // Do nothing
                    }
                    r.close();
                }
                Thread.sleep((long) (sleepBetweenQueries * 1000));
            }

        } catch (Exception ex) {
            // This error should be something like "No results were returned by the query."
            if (LOG.isDebugEnabled()) {
                LOG.debug("Exception for Sleep query. This may be expected!", ex);
            }
        }
    }

}
