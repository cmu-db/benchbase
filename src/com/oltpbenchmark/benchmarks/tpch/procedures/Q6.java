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

public class Q6 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "sum(l_extendedprice * l_discount) as revenue "
            + "from "
            +     "lineitem "
            + "where "
            +     "l_shipdate >= date ? "
            +     "and l_shipdate < date ? + interval '1' year "
            +     "and l_discount between ? - 0.01 and ? + 0.01 "
            +     "and l_quantity < ?"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // DATE is the first of January of a randomly selected year within [1993 .. 1997]
        int year = rand.number(1993, 1997);
        Date date = Date.valueOf(String.format("%d-01-01", year));

        // DISCOUNT is randomly selected within [0.02 .. 0.09]
        double discount = Double.parseDouble(String.format("0.0%d", rand.number(2, 9)));

        // QUANTITY is randomly selected within [24 .. 25]
        int quantity = rand.number(24, 25);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setDate(1, date);
        stmt.setDate(2, date);
        stmt.setDouble(3, discount);
        stmt.setDouble(4, discount);
        stmt.setInt(5, quantity);
        return stmt;
    }
}
