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

public class Q12 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "SELECT o_ol_cnt, "
                    + "sum(CASE WHEN o_carrier_id = 1 "
                    + "OR o_carrier_id = 2 THEN 1 ELSE 0 END) AS high_line_count, "
                    + "sum(CASE WHEN o_carrier_id <> 1 "
                    + "AND o_carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line_count "
                    + "FROM oorder, "
                    + "order_line "
                    + "WHERE ol_w_id = o_w_id "
                    + "AND ol_d_id = o_d_id "
                    + "AND ol_o_id = o_id "
                    + "AND o_entry_d <= ol_delivery_d "
                    + "AND ol_delivery_d < '2020-01-01 00:00:00.000000' "
                    + "GROUP BY o_ol_cnt "
                    + "ORDER BY o_ol_cnt"
    );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
