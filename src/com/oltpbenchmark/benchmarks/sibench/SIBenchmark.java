package com.oltpbenchmark.benchmarks.sibench;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.sibench.procedures.UpdateRecord;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class SIBenchmark extends BenchmarkModule {

    public SIBenchmark(WorkloadConfiguration workConf) {
        super("SI", workConf, true);
    }

    @Override
    protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
        ArrayList<Worker> workers = new ArrayList<Worker>();
        try {
            Connection metaConn = this.makeConnection();

            // LOADING FROM THE DATABASE IMPORTANT INFORMATION
            // LIST OF USERS

            Table t = this.catalog.getTable("SITEST");
            assert (t != null) : "Invalid table name '" + t + "' " + this.catalog.getTables();
            String recordCount = SQLUtil.getMaxColSQL(t, "id");
            Statement stmt = metaConn.createStatement();
            ResultSet res = stmt.executeQuery(recordCount);
            int init_record_count = 0;
            while (res.next()) {
                init_record_count = res.getInt(1);
            }
            assert init_record_count > 0;
            res.close();
            //
            for (int i = 0; i < workConf.getTerminals(); ++i) {
                workers.add(new SIWorker(i, this, init_record_count));
            } // FOR
            metaConn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return workers;
    }

    @Override
    protected Loader makeLoaderImpl(Connection conn) throws SQLException {
        return new SILoader(this, conn);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return UpdateRecord.class.getPackage();
    }

}
