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

public class Q10 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "c_custkey, "
            +     "c_name, "
            +     "sum(l_extendedprice * (1 - l_discount)) as revenue, "
            +     "c_acctbal, "
            +     "n_name, "
            +     "c_address, "
            +     "c_phone, "
            +     "c_comment "
            + "from "
            +     "customer, "
            +     "orders, "
            +     "lineitem, "
            +     "nation "
            + "where "
            +     "c_custkey = o_custkey "
            +     "and l_orderkey = o_orderkey "
            +     "and o_orderdate >= date ? "
            +     "and o_orderdate < date ? + interval '3' month "
            +     "and l_returnflag = 'R' "
            +     "and c_nationkey = n_nationkey "
            + "group by "
            +     "c_custkey, "
            +     "c_name, "
            +     "c_acctbal, "
            +     "c_phone, "
            +     "n_name, "
            +     "c_address, "
            +     "c_comment "
            + "order by "
            +     "revenue desc "
            + "limit 20"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // DATE is the first day of a randomly selected month from the second month of 1993 to the first month of 1995
        int year = rand.number(1993, 1995);
        int month = rand.number(year == 1993 ? 2 : 1, year == 1995 ? 1 : 12);
        Date date = Date.valueOf(String.format("%d-%02d-01", year, month));

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setDate(1, date);
        stmt.setDate(2, date);
        return stmt;
    }
}
