package com.oltpbenchmark.benchmarks.tpch.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q13 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "c_count, "
            +     "count(*) as custdist "
            + "from "
            +     "( "
            +         "select "
            +             "c_custkey, "
            +             "count(o_orderkey) as c_count "
            +         "from "
            +             "customer left outer join orders on "
            +                 "c_custkey = o_custkey "
            +                 "and o_comment not like '%special%deposits%' "
            +         "group by "
            +             "c_custkey "
            +     ") as c_orders "
            + "group by "
            +     "c_count "
            + "order by "
            +     "custdist desc, "
            +     "c_count desc"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
