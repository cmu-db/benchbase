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
public class Region {

    public int r_regionkey; // PRIMARY KEY
    public String r_name;
    public String r_comment;

    @Override
    public String toString() {
        return ("\n***************** Region ********************"
                + "\n*    r_regionkey = " + r_regionkey + "\n*  r_name = " + r_name
                + "\n* r_comment = " + r_comment + "\n**********************************************");
    }

}

//<<< CH-benCHmark