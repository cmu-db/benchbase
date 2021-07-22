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


package com.oltpbenchmark.benchmarks.tpcc.pojo;

import java.sql.Timestamp;

public class OrderLine {

    public int ol_w_id;
    public int ol_d_id;
    public int ol_o_id;
    public int ol_number;
    public int ol_i_id;
    public int ol_supply_w_id;
    public int ol_quantity;
    public Timestamp ol_delivery_d;
    public float ol_amount;
    public String ol_dist_info;

    @Override
    public String toString() {
        return ("\n***************** OrderLine ********************"
                + "\n*        ol_w_id = " + ol_w_id
                + "\n*        ol_d_id = " + ol_d_id
                + "\n*        ol_o_id = " + ol_o_id
                + "\n*      ol_number = " + ol_number
                + "\n*        ol_i_id = " + ol_i_id
                + "\n*  ol_delivery_d = " + ol_delivery_d
                + "\n*      ol_amount = " + ol_amount
                + "\n* ol_supply_w_id = " + ol_supply_w_id
                + "\n*    ol_quantity = " + ol_quantity
                + "\n*   ol_dist_info = " + ol_dist_info
                + "\n**********************************************");
    }

}
