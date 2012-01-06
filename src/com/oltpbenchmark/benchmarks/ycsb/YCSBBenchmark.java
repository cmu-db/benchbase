package com.oltpbenchmark.benchmarks.ycsb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;

public class YCSBBenchmark extends BenchmarkModule{

	public YCSBBenchmark(WorkloadConfiguration workConf) {
		super("ycsb", workConf);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		// TODO Auto-generated method stub
		ArrayList<Worker> workers = new ArrayList<Worker>();
		try {
			for (int i = 0; i < workConf.getTerminals(); ++i) {
				Connection conn = this.getConnection();
				conn.setAutoCommit(false);
				workers.add(new YCSBWorker(i, this));
			} // FOR
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return workers;
	}

	@Override
	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		return new YCSBLoader(this, conn);
	}

	@Override
	protected Package getProcedurePackageImpl() {
		// TODO Auto-generated method stub
		 return InsertRecord.class.getPackage();
	}

}
