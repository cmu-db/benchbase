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

public class Q7 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "SELECT su_nationkey AS supp_nation, "
                    + "substring(c_state from 1 for 1) AS cust_nation, "
                    + "extract(YEAR "
                    + "FROM o_entry_d) AS l_year, "
                    + "sum(ol_amount) AS revenue "
                    + "FROM supplier, "
                    + "stock, "
                    + "order_line, "
                    + "oorder, "
                    + "customer, "
                    + "nation n1, "
                    + "nation n2 "
                    + "WHERE ol_supply_w_id = s_w_id "
                    + "AND ol_i_id = s_i_id "
                    + "AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey "
                    + "AND ol_w_id = o_w_id "
                    + "AND ol_d_id = o_d_id "
                    + "AND ol_o_id = o_id "
                    + "AND c_id = o_c_id "
                    + "AND c_w_id = o_w_id "
                    + "AND c_d_id = o_d_id "
                    + "AND su_nationkey = n1.n_nationkey "
                    + "AND ascii(substring(c_state from  1  for  1)) = n2.n_nationkey "
                    + "AND ((n1.n_name = 'Germany' "
                    + "AND n2.n_name = 'Cambodia') "
                    + "OR (n1.n_name = 'Cambodia' "
                    + "AND n2.n_name = 'Germany')) "
                    + "GROUP BY su_nationkey, "
                    + "cust_nation, "
                    + "l_year "
                    + "ORDER BY su_nationkey, "
                    + "cust_nation, "
                    + "l_year"
    );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
