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
            +     "c_mktsegment = 'MACHINERY' "
            +     "and c_custkey = o_custkey "
            +     "and l_orderkey = o_orderkey "
            +     "and o_orderdate < date '1995-03-10' "
            +     "and l_shipdate > date '1995-03-10' "
            + "group by "
            +     "l_orderkey, "
            +     "o_orderdate, "
            +     "o_shippriority "
            + "order by "
            +     "revenue desc, "
            +     "o_orderdate"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
