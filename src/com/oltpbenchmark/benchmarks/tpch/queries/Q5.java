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

public class Q5 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "n_name, "
            +     "sum(l_extendedprice * (1 - l_discount)) as revenue "
            + "from "
            +     "customer, "
            +     "orders, "
            +     "lineitem, "
            +     "supplier, "
            +     "nation, "
            +     "region "
            + "where "
            +     "c_custkey = o_custkey "
            +     "and l_orderkey = o_orderkey "
            +     "and l_suppkey = s_suppkey "
            +     "and c_nationkey = s_nationkey "
            +     "and s_nationkey = n_nationkey "
            +     "and n_regionkey = r_regionkey "
            +     "and r_name = 'AFRICA' "
            +     "and o_orderdate >= date '1997-01-01' "
            +     "and o_orderdate < date '1997-01-01' + interval '1' year "
            + "group by "
            +     "n_name "
            + "order by "
            +     "revenue desc"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
