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
import com.oltpbenchmark.benchmarks.tpch.TPCHUtil;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q13 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               c_count,
               COUNT(*) AS custdist
            FROM
               (
                  SELECT
                     c_custkey,
                     COUNT(o_orderkey) AS c_count
                  FROM
                     customer
                     LEFT OUTER JOIN
                        orders
                        ON c_custkey = o_custkey
                        AND o_comment NOT LIKE ?
                  GROUP BY
                     c_custkey
               )
               AS c_orders
            GROUP BY
               c_count
            ORDER BY
               custdist DESC,
               c_count DESC
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        // WORD1 is randomly selected from 4 possible values: special, pending, unusual, express
        String word1 = TPCHUtil.choice(new String[]{"special", "pending", "unusual", "express"}, rand);

        // WORD2 is randomly selected from 4 possible values: packages, requests, accounts, deposits
        String word2 = TPCHUtil.choice(new String[]{"packages", "requests", "accounts", "deposits"}, rand);

        String filter = "%" + word1 + "%" + word2 + "%";

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, filter);
        return stmt;
    }
}
