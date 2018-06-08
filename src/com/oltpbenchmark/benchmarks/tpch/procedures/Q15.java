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

import java.sql.*;

public class Q15 extends GenericQuery {

    public final SQLStmt createview_stmt = new SQLStmt(
              "create view revenue0 (supplier_no, total_revenue) as "
            +     "select "
            +         "l_suppkey, "
            +         "sum(l_extendedprice * (1 - l_discount)) "
            +     "from "
            +         "lineitem "
            +     "where "
            +         "l_shipdate >= date ? "
            +         "and l_shipdate < date ? + interval '3' month "
            +     "group by "
            +         "l_suppkey"
        );

    public final SQLStmt query_stmt = new SQLStmt (
              "select "
            +     "s_suppkey, "
            +     "s_name, "
            +     "s_address, "
            +     "s_phone, "
            +     "total_revenue "
            + "from "
            +     "supplier, "
            +     "revenue0 "
            + "where "
            +     "s_suppkey = supplier_no "
            +     "and total_revenue = ( "
            +         "select "
            +             "max(total_revenue) "
            +         "from "
            +             "revenue0 "
            +     ") "
            + "order by "
            +     "s_suppkey"
        );

    public final SQLStmt dropview_stmt = new SQLStmt(
              "drop view revenue0"
        );

    @Override
    public ResultSet run(Connection conn, RandomGenerator rand) throws SQLException {
        // With this query, we have to set up a view before we execute the
        // query, then drop it once we're done.
        Statement stmt = conn.createStatement();
        String sql;
        ResultSet ret = null;
        try {
            // DATE is the first day of a randomly selected month between
            // the first month of 1993 and the 10th month of 1997
            int year = rand.number(1993, 1997);
            int month = rand.number(1, year == 1997 ? 10 : 12);
            String date = String.format("%d-%02d-01", year, month);

            sql = createview_stmt.getSQL();
            sql = sql.replace("?", String.format("'%s'", date));
            stmt.execute(sql);
            ret = super.run(conn, rand);
        } finally {
            sql = dropview_stmt.getSQL();
            stmt.execute(sql);
        }

        return ret;
    }

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        return this.getPreparedStatement(conn, query_stmt);
    }
}
