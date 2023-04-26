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

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpch.TPCHConstants;
import com.oltpbenchmark.benchmarks.tpch.TPCHUtil;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q11 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               ps_partkey,
               SUM(ps_supplycost * ps_availqty) AS VALUE
            FROM
               partsupp,
               supplier,
               nation
            WHERE
               ps_suppkey = s_suppkey
               AND s_nationkey = n_nationkey
               AND n_name = ?
            GROUP BY
               ps_partkey
            HAVING
               SUM(ps_supplycost * ps_availqty) > (
               SELECT
                  SUM(ps_supplycost * ps_availqty) * ?
               FROM
                  partsupp, supplier, nation
               WHERE
                  ps_suppkey = s_suppkey
                  AND s_nationkey = n_nationkey
                  AND n_name = ? )
               ORDER BY
                  VALUE DESC
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        // NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3
        String nation = TPCHUtil.choice(TPCHConstants.N_NAME, rand);

        // FRACTION is chosen as 0.0001 / SF
        double fraction = 0.0001 / scaleFactor;

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, nation);
        stmt.setDouble(2, fraction);
        stmt.setString(3, nation);
        return stmt;
    }
}
