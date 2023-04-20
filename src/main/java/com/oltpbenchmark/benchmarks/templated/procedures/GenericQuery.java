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
package com.oltpbenchmark.benchmarks.templated.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public abstract class GenericQuery extends Procedure {

    protected static final Logger LOG = LoggerFactory.getLogger(GenericQuery.class);

    /** Execution method with parameters. */
    public void run(Connection conn, List<Object> params) throws SQLException {
        try (PreparedStatement stmt = getStatement(conn, params); ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                //do nothing
            }
        }
        conn.commit();
    }

    /** Execution method without parameters. */
    public void run(Connection conn) throws SQLException {
        QueryTemplateInfo queryTemplateInfo = this.getQueryTemplateInfo();

        try (PreparedStatement stmt = this.getPreparedStatement(conn, queryTemplateInfo.getQuery()); ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                //do nothing
            }
        }
        conn.commit();
    }

    public PreparedStatement getStatement(Connection conn, List<Object> params) throws SQLException {
        QueryTemplateInfo queryTemplateInfo = this.getQueryTemplateInfo();

        PreparedStatement stmt = this.getPreparedStatement(conn, queryTemplateInfo.getQuery());
        int[] paramsTypes = queryTemplateInfo.getParamsTypes();
        for (int i = 0; i < paramsTypes.length; i++) {
            switch (paramsTypes[i]) {
            case Types.NULL:
                stmt.setNull(i + 1, paramsTypes[i]);
                break;
            default:
                stmt.setObject(i + 1, params.get(i), paramsTypes[i]);
            }
        }
        return stmt;
    }

    public abstract QueryTemplateInfo getQueryTemplateInfo();

    @Value.Immutable
    public interface QueryTemplateInfo {

        /** Query string for this template. */
        SQLStmt getQuery();

        /** Query parameter types. */
        int[] getParamsTypes();

        /** Potential query parameter values. */
        String[] getParamsValues();
    }

}
