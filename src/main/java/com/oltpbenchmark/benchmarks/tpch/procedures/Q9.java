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
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q9 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "nation, "
            +     "o_year, "
            +     "sum(amount) as sum_profit "
            + "from "
            +     "( "
            +         "select "
            +             "n_name as nation, "
            +             "extract(year from o_orderdate) as o_year, "
            +             "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount "
            +         "from "
            +             "part, "
            +             "supplier, "
            +             "lineitem, "
            +             "partsupp, "
            +             "orders, "
            +             "nation "
            +         "where "
            +             "s_suppkey = l_suppkey "
            +             "and ps_suppkey = l_suppkey "
            +             "and ps_partkey = l_partkey "
            +             "and p_partkey = l_partkey "
            +             "and o_orderkey = l_orderkey "
            +             "and s_nationkey = n_nationkey "
            +             "and p_name like ? "
            +     ") as profit "
            + "group by "
            +     "nation, "
            +     "o_year "
            + "order by "
            +     "nation, "
            +     "o_year desc"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // COLOR is randomly selected within the list of values defined for the generation of P_NAME in Clause 4.2.3
        String color = "%" + TPCHUtil.choice(TPCHConstants.P_NAME_GENERATOR, rand) + "%";

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, color);
        return stmt;
    }
}
