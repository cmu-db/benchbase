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

public class Q19 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "sum(l_extendedprice* (1 - l_discount)) as revenue "
            + "from "
            +     "lineitem, "
            +     "part "
            + "where "
            +     "( "
            +         "p_partkey = l_partkey "
            +         "and p_brand = 'Brand#34' "
            +         "and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
            +         "and l_quantity >= 5 and l_quantity <= 5 + 10 "
            +         "and p_size between 1 and 5 "
            +         "and l_shipmode in ('AIR', 'AIR REG') "
            +         "and l_shipinstruct = 'DELIVER IN PERSON' "
            +     ") "
            +     "or "
            +     "( "
            +         "p_partkey = l_partkey "
            +         "and p_brand = 'Brand#51' "
            +         "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') "
            +         "and l_quantity >= 12 and l_quantity <= 12 + 10 "
            +         "and p_size between 1 and 10 "
            +         "and l_shipmode in ('AIR', 'AIR REG') "
            +         "and l_shipinstruct = 'DELIVER IN PERSON' "
            +     ") "
            +     "or "
            +     "( "
            +         "p_partkey = l_partkey "
            +         "and p_brand = 'Brand#35' "
            +         "and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') "
            +         "and l_quantity >= 30 and l_quantity <= 30 + 10 "
            +         "and p_size between 1 and 15 "
            +         "and l_shipmode in ('AIR', 'AIR REG') "
            +         "and l_shipinstruct = 'DELIVER IN PERSON' "
            +     ")"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
