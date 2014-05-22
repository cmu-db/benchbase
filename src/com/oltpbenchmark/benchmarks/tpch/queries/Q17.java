package com.oltpbenchmark.benchmarks.tpch.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q17 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "sum(l_extendedprice) / 7.0 as avg_yearly "
            + "from "
            +     "lineitem, "
            +     "part "
            + "where "
            +     "p_partkey = l_partkey "
            +     "and p_brand = 'Brand#14' "
            +     "and p_container = 'MED BOX' "
            +     "and l_quantity < ( "
            +         "select "
            +             "0.2 * avg(l_quantity) "
            +         "from "
            +             "lineitem "
            +         "where "
            +             "l_partkey = p_partkey "
            +     ")"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
