package com.oltpbenchmark.benchmarks.tpch.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q12 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt(
              "select "
            +     "l_shipmode, "
            +     "sum(case "
            +         "when o_orderpriority = '1-URGENT' "
            +             "or o_orderpriority = '2-HIGH' "
            +             "then 1 "
            +         "else 0 "
            +     "end) as high_line_count, "
            +     "sum(case "
            +         "when o_orderpriority <> '1-URGENT' "
            +             "and o_orderpriority <> '2-HIGH' "
            +             "then 1 "
            +         "else 0 "
            +     "end) as low_line_count "
            + "from "
            +     "orders, "
            +     "lineitem "
            + "where "
            +     "o_orderkey = l_orderkey "
            +     "and l_shipmode in ('AIR', 'REG AIR') "
            +     "and l_commitdate < l_receiptdate "
            +     "and l_shipdate < l_commitdate "
            +     "and l_receiptdate >= date '1997-01-01' "
            +     "and l_receiptdate < date '1997-01-01' + interval '1' year "
            + "group by "
            +     "l_shipmode "
            + "order by "
            +     "l_shipmode"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }
}
