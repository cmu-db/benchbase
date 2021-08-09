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

import java.io.Serializable;

public class NewOrder implements Serializable {

    public int no_w_id;
    public int no_d_id;
    public int no_o_id;

    @Override
    public String toString() {
        return ("\n***************** NewOrder ********************"
                + "\n*      no_w_id = " + no_w_id + "\n*      no_d_id = "
                + no_d_id + "\n*      no_o_id = " + no_o_id + "\n**********************************************");
    }

}
