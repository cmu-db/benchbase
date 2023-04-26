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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q6 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               SUM(l_extendedprice * l_discount) AS revenue
            FROM
               lineitem
            WHERE
               l_shipdate >= DATE ?
               AND l_shipdate < DATE ? + INTERVAL '1' YEAR
               AND l_discount BETWEEN ? - 0.01 AND ? + 0.01
               AND l_quantity < ?
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        // DATE is the first of January of a randomly selected year within [1993 .. 1997]
        int year = rand.number(1993, 1997);
        String date = String.format("%d-01-01", year);

        // DISCOUNT is randomly selected within [0.02 .. 0.09]
        String discount = String.format("0.0%d", rand.number(2, 9));

        // QUANTITY is randomly selected within [24 .. 25]
        int quantity = rand.number(24, 25);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setDate(1, Date.valueOf(date));
        stmt.setDate(2, Date.valueOf(date));
        stmt.setString(3, discount);
        stmt.setString(4, discount);
        stmt.setInt(5, quantity);
        return stmt;
    }
}
