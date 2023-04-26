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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q3 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               l_orderkey,
               SUM(l_extendedprice * (1 - l_discount)) AS revenue,
               o_orderdate,
               o_shippriority
            FROM
               customer,
               orders,
               lineitem
            WHERE
               c_mktsegment = ?
               AND c_custkey = o_custkey
               AND l_orderkey = o_orderkey
               AND o_orderdate < DATE ?
               AND l_shipdate > DATE ?
            GROUP BY
               l_orderkey,
               o_orderdate,
               o_shippriority
            ORDER BY
               revenue DESC,
               o_orderdate LIMIT 10
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String segment = TPCHUtil.choice(TPCHConstants.SEGMENTS, rand);

        // date must be randomly selected between [1995-03-01, 1995-03-31]
        int day = rand.number(1, 31);
        String date = String.format("1995-03-%02d", day);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, segment);
        stmt.setDate(2, Date.valueOf(date));
        stmt.setDate(3, Date.valueOf(date));
        return stmt;
    }
}
