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
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q1 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
              SELECT
                 l_returnflag,
                 l_linestatus,
                 SUM(l_quantity) AS sum_qty,
                 SUM(l_extendedprice) AS sum_base_price,
                 SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                 SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                 AVG(l_quantity) AS avg_qty,
                 AVG(l_extendedprice) AS avg_price,
                 AVG(l_discount) AS avg_disc,
                 COUNT(*) AS count_order
              FROM
                 lineitem
              WHERE
                 l_shipdate <= DATE '1998-12-01' - INTERVAL ? DAY
              GROUP BY
                 l_returnflag,
                 l_linestatus
              ORDER BY
                 l_returnflag,
                 l_linestatus
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String delta = String.valueOf(rand.number(60, 120));

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, delta);
        return stmt;
    }
}
