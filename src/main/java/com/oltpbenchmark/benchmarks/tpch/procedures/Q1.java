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

public class Q1 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "l_returnflag, "
            +     "l_linestatus, "
            +     "sum(l_quantity) as sum_qty, "
            +     "sum(l_extendedprice) as sum_base_price, "
            +     "sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, "
            +     "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, "
            +     "avg(l_quantity) as avg_qty, "
            +     "avg(l_extendedprice) as avg_price, "
            +     "avg(l_discount) as avg_disc, "
            +     "count(*) as count_order "
            + "from "
            +     "lineitem "
            + "where "
            +     "l_shipdate <= date '1998-12-01' - interval ? day "
            + "group by "
            +     "l_returnflag, "
            +     "l_linestatus "
            + "order by "
            +     "l_returnflag, "
            +     "l_linestatus"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        String delta = String.valueOf(rand.number(60, 120));

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, delta);
        return stmt;
    }
}
