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
            +                 "sum(ps_supplycost * ps_availqty) * 0.0000003333 "
            +             "from "
            +                 "partsupp, "
            +                 "supplier, "
            +                 "nation "
            +             "where "
            +                 "ps_suppkey = s_suppkey "
            +                 "and s_nationkey = n_nationkey "
            +                 "and n_name = 'ETHIOPIA' "
            +         ") "
            + "order by "
            +     "value desc"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
