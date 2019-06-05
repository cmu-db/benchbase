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

public class Q12 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "l_shipmode, "
            +     "sum(case "
            +         "when o_orderpriority = '1-URGENT' "
            +             "or o_orderpriority = '2-HIGH' "
            +             "then 1 "
            +         "else 0 "
            +     "end) as high_line_count, "
            +     "sum(case "
            +         "when o_orderpriority <> '1-URGENT' "
            +             "and o_orderpriority <> '2-HIGH' "
            +             "then 1 "
            +         "else 0 "
            +     "end) as low_line_count "
            + "from "
            +     "orders, "
            +     "lineitem "
            + "where "
            +     "o_orderkey = l_orderkey "
            +     "and l_shipmode in (?, ?) "
            +     "and l_commitdate < l_receiptdate "
            +     "and l_shipdate < l_commitdate "
            +     "and l_receiptdate >= date ? "
            +     "and l_receiptdate < date ? + interval '1' year "
            + "group by "
            +     "l_shipmode "
            + "order by "
            +     "l_shipmode"
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
        Date date = Date.valueOf(String.format("%d-01-01", year));

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, shipMode1);
        stmt.setString(2, shipMode2);
        stmt.setDate(3, date);
        stmt.setDate(4, date);
        return stmt;
    }
}
