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

public class Q2 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
            "SELECT su_suppkey, "
                    + "su_name, "
                    + "n_name, "
                    + "i_id, "
                    + "i_name, "
                    + "su_address, "
                    + "su_phone, "
                    + "su_comment "
                    + "FROM item, supplier, stock, nation, region, "
                    + "(SELECT s_i_id AS m_i_id, MIN(s_quantity) AS m_s_quantity "
                    + "FROM stock, "
                    + "supplier, "
                    + "nation, "
                    + "region "
                    + "WHERE MOD((s_w_id*s_i_id), 10000)=su_suppkey "
                    + "AND su_nationkey=n_nationkey "
                    + "AND n_regionkey=r_regionkey "
                    + "AND r_name LIKE 'Europ%' "
                    + "GROUP BY s_i_id) m "
                    + "WHERE i_id = s_i_id "
                    + "AND MOD((s_w_id * s_i_id), 10000) = su_suppkey "
                    + "AND su_nationkey = n_nationkey "
                    + "AND n_regionkey = r_regionkey "
                    + "AND i_data LIKE '%b' "
                    + "AND r_name LIKE 'Europ%' "
                    + "AND i_id=m_i_id "
                    + "AND s_quantity = m_s_quantity "
                    + "ORDER BY n_name, "
                    + "su_name, "
                    + "i_id"
    );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
