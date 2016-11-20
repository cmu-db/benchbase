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

package com.oltpbenchmark.benchmarks.tpch.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q20 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "s_name, "
            +     "s_address "
            + "from "
            +     "supplier, "
            +     "nation "
            + "where "
            +     "s_suppkey in ( "
            +         "select "
            +             "ps_suppkey "
            +         "from "
            +             "partsupp "
            +         "where "
            +             "ps_partkey in ( "
            +                 "select "
            +                     "p_partkey "
            +                 "from "
            +                     "part "
            +                 "where "
            +                     "p_name like 'orange%' "
            +             ") "
            +             "and ps_availqty > ( "
            +                 "select "
            +                     "0.5 * sum(l_quantity) "
            +                 "from "
            +                     "lineitem "
            +                 "where "
            +                     "l_partkey = ps_partkey "
            +                     "and l_suppkey = ps_suppkey "
            +                     "and l_shipdate >= date '1997-01-01' "
            +                     "and l_shipdate < date '1997-01-01' + interval '1' year "
            +             ") "
            +     ") "
            +     "and s_nationkey = n_nationkey "
            +     "and n_name = 'ALGERIA' "
            + "order by "
            +     "s_name"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
