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

package com.oltpbenchmark.benchmarks.tpch.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.util.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

public abstract class GenericQuery extends Procedure {

    protected static final Logger LOG = LoggerFactory.getLogger(GenericQuery.class);

    protected abstract PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException;

    public void run(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        try (PreparedStatement stmt = getStatement(conn, rand, scaleFactor)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    //do nothing
                }
            }
            catch (SQLSyntaxErrorException ex) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(this.getClass().getName() + ": stmt: " + stmt.toString());
                }
                throw ex;
            }
        }
    }
}
