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

public class Q14 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "SELECT (100.00 * sum(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END) / (1 + sum(ol_amount))) AS promo_revenue "
                    + "FROM order_line, "
                    + "item "
                    + "WHERE ol_i_id = i_id "
                    + "AND ol_delivery_d >= '2007-01-02 00:00:00.000000' "
                    + "AND ol_delivery_d < '2020-01-02 00:00:00.000000'"
    );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
