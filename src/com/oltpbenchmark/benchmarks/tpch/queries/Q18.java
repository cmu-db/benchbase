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

public class Q18 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "c_name, "
            +     "c_custkey, "
            +     "o_orderkey, "
            +     "o_orderdate, "
            +     "o_totalprice, "
            +     "sum(l_quantity) "
            + "from "
            +     "customer, "
            +     "orders, "
            +     "lineitem "
            + "where "
            +     "o_orderkey in ( "
            +         "select "
            +             "l_orderkey "
            +         "from "
            +             "lineitem "
            +         "group by "
            +             "l_orderkey having "
            +                 "sum(l_quantity) > 314 "
            +     ") "
            +     "and c_custkey = o_custkey "
            +     "and o_orderkey = l_orderkey "
            + "group by "
            +     "c_name, "
            +     "c_custkey, "
            +     "o_orderkey, "
            +     "o_orderdate, "
            +     "o_totalprice "
            + "order by "
            +     "o_totalprice desc, "
            +     "o_orderdate"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
