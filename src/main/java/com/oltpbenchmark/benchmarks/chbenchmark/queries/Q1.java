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

public class Q1 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "SELECT ol_number, "
                    + "sum(ol_quantity) AS sum_qty, "
                    + "sum(ol_amount) AS sum_amount, "
                    + "avg(ol_quantity) AS avg_qty, "
                    + "avg(ol_amount) AS avg_amount, "
                    + "count(*) AS count_order "
                    + "FROM order_line "
                    + "WHERE ol_delivery_d > '2007-01-02 00:00:00.000000' "
                    + "GROUP BY ol_number "
                    + "ORDER BY ol_number"
    );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
