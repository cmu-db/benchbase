package com.oltpbenchmark.benchmarks.resourcestresser.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserWorker;

public class CPU2 extends Procedure {

    public final SQLStmt cpuSelect;
    { 
        String complexClause = "passwd";
        for (int i = 1; i <= ResourceStresserWorker.CPU2_nestedLevel; ++i) {
            complexClause = "md5(concat(" + complexClause +",?))";
        } // FOR
        cpuSelect = new SQLStmt(
            "SELECT count(*) FROM (SELECT " + complexClause + " FROM cputable WHERE empid >= 1 AND empid <= 100) AS T2" 
        );
    }
    
    public void run(Connection conn) throws SQLException {
        final int howManyPerTrasaction = ResourceStresserWorker.CPU2_howManyPerTrasaction;
        final int sleepLength = ResourceStresserWorker.CPU2_sleep;
        final int nestedLevel = ResourceStresserWorker.CPU2_nestedLevel;

        PreparedStatement stmt = this.getPreparedStatement(conn, cpuSelect);

        for (int tranIdx = 0; tranIdx < howManyPerTrasaction; ++tranIdx) {
            double randNoise = ResourceStresserWorker.gen.nextDouble();

            for (int i = 1; i <= nestedLevel; ++i) {
                stmt.setString(i, Double.toString(randNoise));
            } // FOR

            // TODO: Is this the right place to sleep?  With rs open???
            ResultSet rs = stmt.executeQuery();
            try {
                Thread.sleep(sleepLength);
            } catch (InterruptedException e) {
                rs.close();
                throw new SQLException("Unexpected interupt while sleeping!");
            }
            rs.close();
        } // FOR
    }
    
}
