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

public class Q19 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               SUM(l_extendedprice* (1 - l_discount)) AS revenue
            FROM
               lineitem,
               part
            WHERE
               (
                  p_partkey = l_partkey
                  AND p_brand = ?
                  AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                  AND l_quantity >= ?
                  AND l_quantity <= ? + 10
                  AND p_size BETWEEN 1 AND 5
                  AND l_shipmode IN ('AIR', 'AIR REG')
                  AND l_shipinstruct = 'DELIVER IN PERSON'
               )
               OR
               (
                  p_partkey = l_partkey
                  AND p_brand = ?
                  AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                  AND l_quantity >= ?
                  AND l_quantity <= ? + 10
                  AND p_size BETWEEN 1 AND 10
                  AND l_shipmode IN ('AIR', 'AIR REG')
                  AND l_shipinstruct = 'DELIVER IN PERSON'
               )
               OR
               (
                  p_partkey = l_partkey
                  AND p_brand = ?
                  AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                  AND l_quantity >= ?
                  AND l_quantity <= ? + 10
                  AND p_size BETWEEN 1 AND 15
                  AND l_shipmode IN ('AIR', 'AIR REG')
                  AND l_shipinstruct = 'DELIVER IN PERSON'
               )
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        // QUANTITY1 is randomly selected within [1..10]
        int quantity1 = rand.number(1, 10);

        // QUANTITY2 is randomly selected within [10..20]
        int quantity2 = rand.number(10, 20);

        // QUANTITY3 is randomly selected within [20..30]
        int quantity3 = rand.number(20, 30);

        // BRAND1, BRAND2, BRAND3 = 'Brand#MN' where each MN is a two character string representing two numbers
        // randomly and independently selected within [1 .. 5]
        String brand1 = TPCHUtil.randomBrand(rand);
        String brand2 = TPCHUtil.randomBrand(rand);
        String brand3 = TPCHUtil.randomBrand(rand);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, brand1);
        stmt.setInt(2, quantity1);
        stmt.setInt(3, quantity1);
        stmt.setString(4, brand2);
        stmt.setInt(5, quantity2);
        stmt.setInt(6, quantity2);
        stmt.setString(7, brand3);
        stmt.setInt(8, quantity3);
        stmt.setInt(9, quantity3);
        return stmt;
    }
}
