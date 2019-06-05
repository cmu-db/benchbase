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
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q18 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "c_name, "
            +     "c_custkey, "
            +     "o_orderkey, "
            +     "o_orderdate, "
            +     "o_totalprice, "
            +     "sum(l_quantity) "
            + "from "
            +     "customer, "
            +     "orders, "
            +     "lineitem "
            + "where "
            +     "o_orderkey in ( "
            +         "select "
            +             "l_orderkey "
            +         "from "
            +             "lineitem "
            +         "group by "
            +             "l_orderkey having "
            +                 "sum(l_quantity) > ? "
            +     ") "
            +     "and c_custkey = o_custkey "
            +     "and o_orderkey = l_orderkey "
            + "group by "
            +     "c_name, "
            +     "c_custkey, "
            +     "o_orderkey, "
            +     "o_orderdate, "
            +     "o_totalprice "
            + "order by "
            +     "o_totalprice desc, "
            +     "o_orderdate "
            + "limit 100"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // QUANTITY is randomly selected within [312..315]
        int quantity = rand.number(312, 315);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setInt(1, quantity);
        return stmt;
    }
}
