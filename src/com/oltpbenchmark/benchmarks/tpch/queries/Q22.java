/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpch.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q22 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "cntrycode, "
            +     "count(*) as numcust, "
            +     "sum(c_acctbal) as totacctbal "
            + "from "
            +     "( "
            +         "select "
            +             "substring(c_phone from 1 for 2) as cntrycode, "
            +             "c_acctbal "
            +         "from "
            +             "customer "
            +         "where "
            +             "substring(c_phone from 1 for 2) in "
            +                 "('20', '32', '44', '33', '29', '22', '31') "
            +             "and c_acctbal > ( "
            +                 "select "
            +                     "avg(c_acctbal) "
            +                 "from "
            +                     "customer "
            +                 "where "
            +                     "c_acctbal > 0.00 "
            +                     "and substring(c_phone from 1 for 2) in "
            +                         "('20', '32', '44', '33', '29', '22', '31') "
            +             ") "
            +             "and not exists ( "
            +                 "select "
            +                     "* "
            +                 "from "
            +                     "orders "
            +                 "where "
            +                     "o_custkey = c_custkey "
            +             ") "
            +     ") as custsale "
            + "group by "
            +     "cntrycode "
            + "order by "
            +     "cntrycode"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
