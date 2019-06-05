/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpch.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpch.util.TPCHConstants;
import com.oltpbenchmark.benchmarks.tpch.util.TPCHUtil;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q20 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "s_name, "
            +     "s_address "
            + "from "
            +     "supplier, "
            +     "nation "
            + "where "
            +     "s_suppkey in ( "
            +         "select "
            +             "ps_suppkey "
            +         "from "
            +             "partsupp "
            +         "where "
            +             "ps_partkey in ( "
            +                 "select "
            +                     "p_partkey "
            +                 "from "
            +                     "part "
            +                 "where "
            +                     "p_name like ? "
            +             ") "
            +             "and ps_availqty > ( "
            +                 "select "
            +                     "0.5 * sum(l_quantity) "
            +                 "from "
            +                     "lineitem "
            +                 "where "
            +                     "l_partkey = ps_partkey "
            +                     "and l_suppkey = ps_suppkey "
            +                     "and l_shipdate >= date ? "
            +                     "and l_shipdate < date ? + interval '1' year "
            +             ") "
            +     ") "
            +     "and s_nationkey = n_nationkey "
            +     "and n_name = ? "
            + "order by "
            +     "s_name"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // COLOR is randomly selected within the list of values defined for the generation of P_NAME
        String color = TPCHUtil.choice(TPCHConstants.P_NAME_GENERATOR, rand) + "%";

        // DATE is the first of January of a randomly selected year within 1993..1997
        int year = rand.number(1993, 1997);
        Date date = Date.valueOf(String.format("%d-01-01", year));

        // NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3
        String nation = TPCHUtil.choice(TPCHConstants.N_NAME, rand);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, color);
        stmt.setDate(2, date);
        stmt.setDate(3, date);
        stmt.setString(4, nation);
        return stmt;
    }
}
