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

public class Q8 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               o_year,
               SUM(
               CASE
                  WHEN
                     nation = ?
                  THEN
                     volume
                  ELSE
                     0
               END
            ) / SUM(volume) AS mkt_share
            FROM
               (
                  SELECT
                     EXTRACT(YEAR
                  FROM
                     o_orderdate) AS o_year,
                     l_extendedprice * (1 - l_discount) AS volume,
                     n2.n_name AS nation
                  FROM
                     part,
                     supplier,
                     lineitem,
                     orders,
                     customer,
                     nation n1,
                     nation n2,
                     region
                  WHERE
                     p_partkey = l_partkey
                     AND s_suppkey = l_suppkey
                     AND l_orderkey = o_orderkey
                     AND o_custkey = c_custkey
                     AND c_nationkey = n1.n_nationkey
                     AND n1.n_regionkey = r_regionkey
                     AND r_name = ?
                     AND s_nationkey = n2.n_nationkey
                     AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                     AND p_type = ?
               )
               AS all_nations
            GROUP BY
               o_year
            ORDER BY
               o_year
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        // NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3
        String nation = TPCHUtil.choice(TPCHConstants.N_NAME, rand);

        // REGION is the value defined in Clause 4.2.3 for R_NAME where R_REGIONKEY corresponds to
        // N_REGIONKEY for the selected NATION in item 1 above
        int n_regionkey = TPCHUtil.getRegionKeyFromNation(nation);
        String region = TPCHUtil.getRegionFromRegionKey(n_regionkey);

        // TYPE is randomly selected within the list of 3-syllable strings defined for Types in Clause 4.2.2.13
        String syllable1 = TPCHUtil.choice(TPCHConstants.TYPE_S1, rand);
        String syllable2 = TPCHUtil.choice(TPCHConstants.TYPE_S2, rand);
        String syllable3 = TPCHUtil.choice(TPCHConstants.TYPE_S3, rand);
        String type = String.format("%s %s %s", syllable1, syllable2, syllable3);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, nation);
        stmt.setString(2, region);
        stmt.setString(3, type);
        return stmt;
    }
}
