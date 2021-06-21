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

public class Q19 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "SELECT sum(ol_amount) AS revenue "
                    + "FROM order_line, "
                    + "item "
                    + "WHERE (ol_i_id = i_id "
                    + "AND i_data LIKE '%a' "
                    + "AND ol_quantity >= 1 "
                    + "AND ol_quantity <= 10 "
                    + "AND i_price BETWEEN 1 AND 400000 "
                    + "AND ol_w_id IN (1, "
                    + "2, "
                    + "3)) "
                    + "OR (ol_i_id = i_id "
                    + "AND i_data LIKE '%b' "
                    + "AND ol_quantity >= 1 "
                    + "AND ol_quantity <= 10 "
                    + "AND i_price BETWEEN 1 AND 400000 "
                    + "AND ol_w_id IN (1, "
                    + "2, "
                    + "4)) "
                    + "OR (ol_i_id = i_id "
                    + "AND i_data LIKE '%c' "
                    + "AND ol_quantity >= 1 "
                    + "AND ol_quantity <= 10 "
                    + "AND i_price BETWEEN 1 AND 400000 "
                    + "AND ol_w_id IN (1, "
                    + "5, "
                    + "3))"
    );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
