package com.oltpbenchmark.benchmarks.tpch.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q6 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "sum(l_extendedprice * l_discount) as revenue "
            + "from "
            +     "lineitem "
            + "where "
            +     "l_shipdate >= date '1997-01-01' "
            +     "and l_shipdate < date '1997-01-01' + interval '1' year "
            +     "and l_discount between 0.07 - 0.01 and 0.07 + 0.01 "
            +     "and l_quantity < 24"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
