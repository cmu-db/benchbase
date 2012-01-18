package com.oltpbenchmark.benchmarks.jpab;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.jpab.procedures.BasicTest;

public class JpabBenchmark extends BenchmarkModule{
    private static final Logger LOG = Logger.getLogger(JpabBenchmark.class);
    
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
	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		LOG.warn("No loading phase needed in this Benchmark");
		return (null);
	}

	@Override
	protected Package getProcedurePackageImpl() {
		 return BasicTest.class.getPackage();
	}

}
