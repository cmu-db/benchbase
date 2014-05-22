package com.oltpbenchmark.benchmarks.tpch.queries;

import com.oltpbenchmark.api.SQLStmt;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

public class Q15 extends GenericQuery {

    public final SQLStmt createview_stmt = new SQLStmt(
              "create view revenue0 (supplier_no, total_revenue) as "
            +     "select "
            +         "l_suppkey, "
            +         "sum(l_extendedprice * (1 - l_discount)) "
            +     "from "
            +         "lineitem "
            +     "where "
            +         "l_shipdate >= date '1997-03-01' "
            +         "and l_shipdate < date '1997-03-01' + interval '3' month "
            +     "group by "
            +         "l_suppkey"
        );

    public final SQLStmt query_stmt = new SQLStmt (
              "select "
            +     "s_suppkey, "
            +     "s_name, "
            +     "s_address, "
            +     "s_phone, "
            +     "total_revenue "
            + "from "
            +     "supplier, "
            +     "revenue0 "
            + "where "
            +     "s_suppkey = supplier_no "
            +     "and total_revenue = ( "
            +         "select "
            +             "max(total_revenue) "
            +         "from "
            +             "revenue0 "
            +     ") "
            + "order by "
            +     "s_suppkey"
        );

    public final SQLStmt dropview_stmt = new SQLStmt(
              "drop view revenue0"
        );

    protected SQLStmt get_query() {
        return query_stmt;
    }

    public ResultSet run(Connection conn) throws SQLException {
        // With this query, we have to set up a view before we execute the
        // query, then drop it once we're done.
        Statement stmt = conn.createStatement();
        ResultSet ret = null;
        try {
            stmt.executeUpdate(createview_stmt.getSQL());
            ret = super.run(conn);
        } finally {
            stmt.executeUpdate(dropview_stmt.getSQL());
        }

        return ret;
    }
}
