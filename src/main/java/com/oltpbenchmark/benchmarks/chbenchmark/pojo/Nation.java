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
public class Nation {

    public int n_nationkey; // PRIMARY KEY
    public String n_name;
    public int n_regionkey;
    public String n_comment;

    @Override
    public String toString() {
        return ("\n***************** Nation ********************"
                + "\n*    n_nationkey = " + n_nationkey + "\n*  n_name = " + n_name
                + "\n*    n_regionkey = " + n_regionkey + "\n*  n_comment = " + n_comment
                + "\n**********************************************");
    }

}

//<<< CH-benCHmark