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

public class Q13 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "c_count, "
            +     "count(*) as custdist "
            + "from "
            +     "( "
            +         "select "
            +             "c_custkey, "
            +             "count(o_orderkey) as c_count "
            +         "from "
            +             "customer left outer join orders on "
            +                 "c_custkey = o_custkey "
            +                 "and o_comment not like '%special%deposits%' "
            +         "group by "
            +             "c_custkey "
            +     ") as c_orders "
            + "group by "
            +     "c_count "
            + "order by "
            +     "custdist desc, "
            +     "c_count desc"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
