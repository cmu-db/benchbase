package com.oltpbenchmark.benchmarks.jpab;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.jpab.procedures.BasicTest;
import com.oltpbenchmark.catalog.Table;

public class JpabBenchmark extends BenchmarkModule{
	
	private EntityManagerFactory emf;
	
	public JpabBenchmark(WorkloadConfiguration workConf) {
		super("jpab", workConf);
	}

	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		ArrayList<Worker> workers = new ArrayList<Worker>();
		emf = Persistence.createEntityManagerFactory("Hibernate-MySQL-server");
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			JpabWorker worker= new JpabWorker(i, this);
			worker.em= emf.createEntityManager();
			workers.add(worker);
		}
		return workers;
	}

	@Override
	protected void loadDatabaseImpl(Connection conn, Map<String, Table> tables)
			throws SQLException {
		System.out.println("No loading phase needed in this Benchmark");
	}

	@Override
	protected Package getProcedurePackageImpl() {
		 return BasicTest.class.getPackage();
	}

}
