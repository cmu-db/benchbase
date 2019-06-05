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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q4 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "o_orderpriority, "
            +     "count(*) as order_count "
            + "from "
            +     "orders "
            + "where "
            +     "o_orderdate >= date ? "
            +     "and o_orderdate < date ? + interval '3' month "
            +     "and exists ( "
            +         "select "
            +             "* "
            +         "from "
            +             "lineitem "
            +         "where "
            +             "l_orderkey = o_orderkey "
            +             "and l_commitdate < l_receiptdate "
            +     ") "
            + "group by "
            +     "o_orderpriority "
            + "order by "
            +     "o_orderpriority"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        int year = rand.number(1993, 1997);
        int month = rand.number(1, 10);
        Date date = Date.valueOf(String.format("%d-%02d-01", year, month));

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setDate(1, date);
        stmt.setDate(2, date);
        return stmt;
    }
}
