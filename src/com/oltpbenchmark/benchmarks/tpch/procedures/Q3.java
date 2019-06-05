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

public class Q3 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "l_orderkey, "
            +     "sum(l_extendedprice * (1 - l_discount)) as revenue, "
            +     "o_orderdate, "
            +     "o_shippriority "
            + "from "
            +     "customer, "
            +     "orders, "
            +     "lineitem "
            + "where "
            +     "c_mktsegment = ? "
            +     "and c_custkey = o_custkey "
            +     "and l_orderkey = o_orderkey "
            +     "and o_orderdate < date ? "
            +     "and l_shipdate > date ? "
            + "group by "
            +     "l_orderkey, "
            +     "o_orderdate, "
            +     "o_shippriority "
            + "order by "
            +     "revenue desc, "
            +     "o_orderdate "
            + "limit 10"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        String segment = TPCHUtil.choice(TPCHConstants.SEGMENTS, rand);

        // date must be randomly selected between [1995-03-01, 1995-03-31]
        int day = rand.number(1, 31);
        Date date = Date.valueOf(String.format("1995-03-%02d", day));

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, segment);
        stmt.setDate(2, date);
        stmt.setDate(3, date);
        return stmt;
    }
}
