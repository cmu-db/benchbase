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

public class Q2 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
             SELECT
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
             FROM
                part,
                supplier,
                partsupp,
                nation,
                region
             WHERE
                p_partkey = ps_partkey
                AND s_suppkey = ps_suppkey
                AND p_size = ?
                AND p_type LIKE ?
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = ?
                AND ps_supplycost =
                (
                   SELECT
                      MIN(ps_supplycost)
                   FROM
                      partsupp,
                      supplier,
                      nation,
                      region
                   WHERE
                      p_partkey = ps_partkey
                      AND s_suppkey = ps_suppkey
                      AND s_nationkey = n_nationkey
                      AND n_regionkey = r_regionkey
                      AND r_name = ?
                )
             ORDER BY
                s_acctbal DESC,
                n_name,
                s_name,
                p_partkey LIMIT 100
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        int size = rand.number(1, 50);
        String type = TPCHUtil.choice(TPCHConstants.TYPE_S3, rand);
        String region = TPCHUtil.choice(TPCHConstants.R_NAME, rand);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setInt(1, size);
        stmt.setString(2, "%" + type);
        stmt.setString(3, region);
        stmt.setString(4, region);
        return stmt;
    }
}
