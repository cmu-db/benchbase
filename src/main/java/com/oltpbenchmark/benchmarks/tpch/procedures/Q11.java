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

public class Q11 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "ps_partkey, "
            +     "sum(ps_supplycost * ps_availqty) as value "
            + "from "
            +     "partsupp, "
            +     "supplier, "
            +     "nation "
            + "where "
            +     "ps_suppkey = s_suppkey "
            +     "and s_nationkey = n_nationkey "
            +     "and n_name = 'ETHIOPIA' "
            + "group by "
            +     "ps_partkey having "
            +         "sum(ps_supplycost * ps_availqty) > ( "
            +             "select "
            +                 "sum(ps_supplycost * ps_availqty) * ? "
            +             "from "
            +                 "partsupp, "
            +                 "supplier, "
            +                 "nation "
            +             "where "
            +                 "ps_suppkey = s_suppkey "
            +                 "and s_nationkey = n_nationkey "
            +                 "and n_name = ? "
            +         ") "
            + "order by "
            +     "value desc"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3
        String nation = TPCHUtil.choice(TPCHConstants.N_NAME, rand);

        // FRACTION is chosen as 0.0001 / SF
        // TODO: we should technically pass dbgen's SF down here somehow
        double fraction = 0.0001;

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setDouble(1, fraction);
        stmt.setString(2, nation);
        return stmt;
    }
}
