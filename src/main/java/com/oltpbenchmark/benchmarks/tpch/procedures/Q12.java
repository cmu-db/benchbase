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

public class Q12 extends GenericQuery {

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
               AND n_name = 'ETHIOPIA'
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
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // SHIPMODE1 is randomly selected within the list of values defined for Modes in Clause 4.2.2.13
        String shipMode1 = TPCHUtil.choice(TPCHConstants.MODES, rand);

        // SHIPMODE2 is randomly selected within the list of values defined for Modes in Clause 4.2.2.13 and must be
        // different from the value selected for SHIPMODE1 in item 1
        String shipMode2 = shipMode1;
        while (shipMode1.equals(shipMode2)) {
            shipMode2 = TPCHUtil.choice(TPCHConstants.MODES, rand);
        }

        // DATE is the first of January of a randomly selected year within [1993 .. 1997]
        int year = rand.number(1993, 1997);
        String date = String.format("%d-01-01", year);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, shipMode1);
        stmt.setString(2, shipMode2);
        stmt.setDate(3, Date.valueOf(date));
        stmt.setDate(4, Date.valueOf(date));
        return stmt;
    }
}
