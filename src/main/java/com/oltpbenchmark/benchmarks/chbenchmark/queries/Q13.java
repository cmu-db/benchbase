/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q13 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "SELECT c_count, "
                    + "count(*) AS custdist "
                    + "FROM "
                    + "(SELECT c_id, "
                    + "count(o_id) AS c_count "
                    + "FROM customer "
                    + "LEFT OUTER JOIN oorder ON (c_w_id = o_w_id "
                    + "AND c_d_id = o_d_id "
                    + "AND c_id = o_c_id "
                    + "AND o_carrier_id > 8) "
                    + "GROUP BY c_id) AS c_orders "
                    + "GROUP BY c_count "
                    + "ORDER BY custdist DESC, c_count DESC"
    );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
