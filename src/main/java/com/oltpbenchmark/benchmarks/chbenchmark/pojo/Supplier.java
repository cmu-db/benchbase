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


package com.oltpbenchmark.benchmarks.chbenchmark.pojo;

//>>> CH-benCHmark
public class Supplier {

    public int su_suppkey; // PRIMARY KEY
    public String su_name;
    public String su_address;
    public int su_nationkey;
    public String su_phone;
    public float su_acctbal;
    public String su_comment;

    @Override
    public String toString() {
        return ("\n***************** Supplier ********************"
                + "\n*    su_suppkey = " + su_suppkey + "\n*  su_name = " + su_name
                + "\n*    su_address = " + su_address + "\n*  su_nationkey = " + su_nationkey
                + "\n*    su_phone = " + su_phone + "\n*  su_acctbal = " + su_acctbal
                + "\n* su_comment = " + su_comment + "\n**********************************************");
    }

}

//<<< CH-benCHmark