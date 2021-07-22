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

public class Q5 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "SELECT n_name, "
                    + "sum(ol_amount) AS revenue "
                    + "FROM customer, "
                    + "oorder, "
                    + "order_line, "
                    + "stock, "
                    + "supplier, "
                    + "nation, "
                    + "region "
                    + "WHERE c_id = o_c_id "
                    + "AND c_w_id = o_w_id "
                    + "AND c_d_id = o_d_id "
                    + "AND ol_o_id = o_id "
                    + "AND ol_w_id = o_w_id "
                    + "AND ol_d_id=o_d_id "
                    + "AND ol_w_id = s_w_id "
                    + "AND ol_i_id = s_i_id "
                    + "AND MOD((s_w_id * s_i_id), 10000) = su_suppkey "
                    + "AND ascii(substring(c_state from  1  for  1)) = su_nationkey "
                    + "AND su_nationkey = n_nationkey "
                    + "AND n_regionkey = r_regionkey "
                    + "AND r_name = 'Europe' "
                    + "AND o_entry_d >= '2007-01-02 00:00:00.000000' "
                    + "GROUP BY n_name "
                    + "ORDER BY revenue DESC"
    );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
