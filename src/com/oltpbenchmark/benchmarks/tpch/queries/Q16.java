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

public class Q16 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "p_brand, "
            +     "p_type, "
            +     "p_size, "
            +     "count(distinct ps_suppkey) as supplier_cnt "
            + "from "
            +     "partsupp, "
            +     "part "
            + "where "
            +     "p_partkey = ps_partkey "
            +     "and p_brand <> 'Brand#41' "
            +     "and p_type not like 'ECONOMY BURNISHED%' "
            +     "and p_size in (22, 33, 42, 5, 27, 49, 4, 18) "
            +     "and ps_suppkey not in ( "
            +         "select "
            +             "s_suppkey "
            +         "from "
            +             "supplier "
            +         "where "
            +             "s_comment like '%Customer%Complaints%' "
            +     ") "
            + "group by "
            +     "p_brand, "
            +     "p_type, "
            +     "p_size "
            + "order by "
            +     "supplier_cnt desc, "
            +     "p_brand, "
            +     "p_type, "
            +     "p_size"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
