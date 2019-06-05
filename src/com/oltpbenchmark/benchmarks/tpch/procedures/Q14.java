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

public class Q14 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "100.00 * sum(case "
            +         "when p_type like 'PROMO%' "
            +             "then l_extendedprice * (1 - l_discount) "
            +         "else 0 "
            +     "end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue "
            + "from "
            +     "lineitem, "
            +     "part "
            + "where "
            +     "l_partkey = p_partkey "
            +     "and l_shipdate >= date ? "
            +     "and l_shipdate < date ? + interval '1' month"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // DATE is the first day of a month randomly selected from a random year within [1993 .. 1997]
        int year = rand.number(1993, 1997);
        int month = rand.number(1, 12);
        Date date = Date.valueOf(String.format("%d-%02d-01", year, month));

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setDate(1, date);
        stmt.setDate(2, date);
        return stmt;
    }
}
