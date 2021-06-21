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

package com.oltpbenchmark.benchmarks.twitter.util;

import com.oltpbenchmark.util.Histogram;

/**
 * A histogram of Twitter username length. This is derived from
 * http://simplymeasured.com/blog/2010/06/lakers-vs-celtics-social-media-breakdown-nba/
 *
 * @author pavlo
 */
public class NameHistogram extends Histogram<Integer> {

    {
        this.put(1, 2);
        this.put(2, 12);
        this.put(3, 209);
        this.put(4, 2027);
        this.put(5, 7987);
        this.put(6, 22236);
        this.put(7, 38682);
        this.put(8, 54809);
        this.put(9, 65614);
        this.put(10, 70547);
        this.put(11, 69153);
        this.put(12, 63777);
        this.put(13, 56049);
        this.put(14, 47905);
        this.put(15, 48166);
    }

}
