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
import java.util.HashSet;
import java.util.Set;

public class Q16 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               p_brand,
               p_type,
               p_size,
               COUNT(DISTINCT ps_suppkey) AS supplier_cnt
            FROM
               partsupp,
               part
            WHERE
               p_partkey = ps_partkey
               AND p_brand <> ?
               AND p_type NOT LIKE ?
               AND p_size IN (?, ?, ?, ?, ?, ?, ?, ?)
               AND ps_suppkey NOT IN
               (
                  SELECT
                     s_suppkey
                  FROM
                     supplier
                  WHERE
                     s_comment LIKE '%Customer%Complaints%'
               )
            GROUP BY
               p_brand,
               p_type,
               p_size
            ORDER BY
               supplier_cnt DESC,
               p_brand,
               p_type,
               p_size
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String brand = TPCHUtil.randomBrand(rand);

        // TYPE is made of the first 2 syllables of a string randomly selected within the
        // list of 3-syllable strings defined for Types in Clause 4.2.2.13
        String syllable1 = TPCHUtil.choice(TPCHConstants.TYPE_S1, rand);
        String syllable2 = TPCHUtil.choice(TPCHConstants.TYPE_S2, rand);
        String type = String.format("%s %s", syllable1, syllable2) + "%";

        // SIZE_n is randomly selected as a set of eight different values within [1 .. 50]
        // for n in [1,8]

        int[] sizes = new int[8];
        Set<Integer> seen = new HashSet<>(8);

        for (int i = 0; i < 8; i++) {
            int num = rand.number(1, 50);

            while (seen.contains(num)) {
                num = rand.number(1, 50);
            }

            sizes[i] = num;
            seen.add(num);
        }

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, brand);
        stmt.setString(2, type);
        for (int i = 0; i < 8; i++) {
            stmt.setInt(3 + i, sizes[i]);
        }
        return stmt;
    }
}
