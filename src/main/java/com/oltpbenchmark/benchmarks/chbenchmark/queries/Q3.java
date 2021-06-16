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

public class Q3 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "SELECT ol_o_id, "
                    + "ol_w_id, "
                    + "ol_d_id, "
                    + "sum(ol_amount) AS revenue, "
                    + "o_entry_d "
                    + "FROM customer, "
                    + "new_order, "
                    + "oorder, "
                    + "order_line "
                    + "WHERE c_state LIKE 'A%' "
                    + "AND c_id = o_c_id "
                    + "AND c_w_id = o_w_id "
                    + "AND c_d_id = o_d_id "
                    + "AND no_w_id = o_w_id "
                    + "AND no_d_id = o_d_id "
                    + "AND no_o_id = o_id "
                    + "AND ol_w_id = o_w_id "
                    + "AND ol_d_id = o_d_id "
                    + "AND ol_o_id = o_id "
                    + "AND o_entry_d > '2007-01-02 00:00:00.000000' "
                    + "GROUP BY ol_o_id, "
                    + "ol_w_id, "
                    + "ol_d_id, "
                    + "o_entry_d "
                    + "ORDER BY revenue DESC , o_entry_d"
    );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
