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

public class Q17 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "sum(l_extendedprice) / 7.0 as avg_yearly "
            + "from "
            +     "lineitem, "
            +     "part "
            + "where "
            +     "p_partkey = l_partkey "
            +     "and p_brand = ? "
            +     "and p_container = ? "
            +     "and l_quantity < ( "
            +         "select "
            +             "0.2 * avg(l_quantity) "
            +         "from "
            +             "lineitem "
            +         "where "
            +             "l_partkey = p_partkey "
            +     ")"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // BRAND = 'Brand#MN' where MN is a two character string representing two numbers randomly and independently
        // selected within [1 .. 5]
        int M = rand.number(1, 5);
        int N = rand.number(1, 5);
        String brand = String.format("BRAND#%d%d", M, N);

        // CONTAINER is randomly selected within the list of 2-syllable strings defined for Containers in Clause
        // 4.2.2.13
        String containerS1 = TPCHUtil.choice(TPCHConstants.CONTAINERS_S1, rand);
        String containerS2 = TPCHUtil.choice(TPCHConstants.CONTAINERS_S2, rand);
        String container = String.format("%s %s", containerS1, containerS2);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, brand);
        stmt.setString(2, container);
        return stmt;
    }
}
