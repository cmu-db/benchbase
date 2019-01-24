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

public class Q8 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "o_year, "
            +     "sum(case "
            +         "when nation = ? then volume "
            +         "else 0 "
            +     "end) / sum(volume) as mkt_share "
            + "from "
            +     "( "
            +         "select "
            +             "extract(year from o_orderdate) as o_year, "
            +             "l_extendedprice * (1 - l_discount) as volume, "
            +             "n2.n_name as nation "
            +         "from "
            +             "part, "
            +             "supplier, "
            +             "lineitem, "
            +             "orders, "
            +             "customer, "
            +             "nation n1, "
            +             "nation n2, "
            +             "region "
            +         "where "
            +             "p_partkey = l_partkey "
            +             "and s_suppkey = l_suppkey "
            +             "and l_orderkey = o_orderkey "
            +             "and o_custkey = c_custkey "
            +             "and c_nationkey = n1.n_nationkey "
            +             "and n1.n_regionkey = r_regionkey "
            +             "and r_name = ? "
            +             "and s_nationkey = n2.n_nationkey "
            +             "and o_orderdate between date '1995-01-01' and date '1996-12-31' "
            +             "and p_type = ? "
            +     ") as all_nations "
            + "group by "
            +     "o_year "
            + "order by "
            +     "o_year"
        );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3
        String nation = TPCHUtil.choice(TPCHConstants.N_NAME, rand);

        // REGION is the value defined in Clause 4.2.3 for R_NAME where R_REGIONKEY corresponds to
        // N_REGIONKEY for the selected NATION in item 1 above
        int n_regionkey = TPCHUtil.getRegionKeyFromNation(nation);
        String region = TPCHUtil.getRegionFromRegionKey(n_regionkey);

        // TYPE is randomly selected within the list of 3-syllable strings defined for Types in Clause 4.2.2.13
        String syllable1 = TPCHUtil.choice(TPCHConstants.TYPE_S1, rand);
        String syllable2 = TPCHUtil.choice(TPCHConstants.TYPE_S2, rand);
        String syllable3 = TPCHUtil.choice(TPCHConstants.TYPE_S3, rand);
        String type = String.format("%s %s %s", syllable1, syllable2, syllable3);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, nation);
        stmt.setString(2, region);
        stmt.setString(3, type);
        return stmt;
    }
}
