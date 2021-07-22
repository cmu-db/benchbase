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

public class Q21 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "SELECT su_name, "
                    + "count(*) AS numwait "
                    + "FROM supplier, "
                    + "order_line l1, "
                    + "oorder, "
                    + "stock, "
                    + "nation "
                    + "WHERE ol_o_id = o_id "
                    + "AND ol_w_id = o_w_id "
                    + "AND ol_d_id = o_d_id "
                    + "AND ol_w_id = s_w_id "
                    + "AND ol_i_id = s_i_id "
                    + "AND mod((s_w_id * s_i_id),10000) = su_suppkey "
                    + "AND l1.ol_delivery_d > o_entry_d "
                    + "AND NOT EXISTS "
                    + "(SELECT * "
                    + "FROM order_line l2 "
                    + "WHERE l2.ol_o_id = l1.ol_o_id "
                    + "AND l2.ol_w_id = l1.ol_w_id "
                    + "AND l2.ol_d_id = l1.ol_d_id "
                    + "AND l2.ol_delivery_d > l1.ol_delivery_d) "
                    + "AND su_nationkey = n_nationkey "
                    + "AND n_name = 'Germany' "
                    + "GROUP BY su_name "
                    + "ORDER BY numwait DESC, su_name"
    );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
