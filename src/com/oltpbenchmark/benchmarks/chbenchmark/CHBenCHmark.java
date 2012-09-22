package com.oltpbenchmark.benchmarks.chbenchmark;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q1;
import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;

public class CHBenCHmark extends BenchmarkModule {
	private static final Logger LOG = Logger.getLogger(CHBenCHmark.class);
	
	static {
		if (!(new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/chbenchmark-ddl.sql")).exists()) {
			copyFile(new File("src", "com/oltpbenchmark/benchmarks/tpcc/tpcc-ddl.sql"), new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/chbenchmark-ddl.sql"));
			copyFile(new File("src", "com/oltpbenchmark/benchmarks/tpcc/tpcc-mysql-ddl.sql"), new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/chbenchmark-mysql-ddl.sql"));
			appendToFile(new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/chbenchmark-mysql-ddl.sql"),""
					+ "DROP TABLE IF EXISTS region;\n"
					+ "DROP TABLE IF EXISTS nation;\n"
					+ "DROP TABLE IF EXISTS supplier;\n", true);
			appendToFile(new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/chbenchmark-mysql-ddl.sql"),""
					+ "create table region (\n"
					+ "   r_regionkey int not null,\n"
					+ "   r_name char(55) not null,\n"
					+ "   r_comment char(152) not null,\n"
					+ "   PRIMARY KEY ( r_regionkey )\n"
					+ ");\n"
					+ "\n"
					+ "create table nation (\n"
					+ "   n_nationkey int not null,\n"
					+ "   n_name char(25) not null,\n"
					+ "   n_regionkey int not null references region(r_regionkey),\n"
					+ "   n_comment char(152) not null,\n"
					+ "   PRIMARY KEY ( n_nationkey )\n"
					+ ");\n"
					+ "\n"
					+ "create table supplier (\n"
					+ "   su_suppkey int not null,\n"
					+ "   su_name char(25) not null,\n"
					+ "   su_address varchar(40) not null,\n"
					+ "   su_nationkey int not null references nation(n_nationkey),\n"
					+ "   su_phone char(15) not null,\n"
					+ "   su_acctbal numeric(12,2) not null,\n"
					+ "   su_comment char(101) not null,\n"
					+ "   PRIMARY KEY ( su_suppkey )\n"
					+ ");\n", false);
		}
	}

	public CHBenCHmark(WorkloadConfiguration workConf) {
		super("chbenchmark", workConf, true);
	}
	
	protected Package getProcedurePackageImpl() {
		return (Q1.class.getPackage());
	}
	
	/**
	 * @param Bool
	 */
	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		// HACK: Turn off terminal messages
		jTPCCConfig.TERMINAL_MESSAGES = false;
		ArrayList<Worker> workers = new ArrayList<Worker>();

		try {
			List<CHBenCHmarkWorker> terminals = createCHTerminals();
			workers.addAll(terminals);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return workers;
	}
	
	protected ArrayList<CHBenCHmarkWorker> createCHTerminals() throws SQLException {

		int numTerminals = workConf.getTerminals();

		ArrayList<CHBenCHmarkWorker> ret = new ArrayList<CHBenCHmarkWorker>();
		for (int i = 0; i < numTerminals; i++)
			ret.add(new CHBenCHmarkWorker(this));
		return ret;
	}

	private static final void copyFile(File source, File target) {
		try {
			FileOutputStream fos = new FileOutputStream(target, false);
			BufferedReader br = new BufferedReader(new FileReader(source));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			String line = br.readLine();
			while (line != null) {
				bw.write(line);
				bw.write("\n");
				line = br.readLine();
			}
			br.close();
			bw.flush();
			bw.close();
			fos.getFD().sync();
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage());
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
	}
	
	private static final void appendToFile(File target, String text, boolean prependNotPostpend) {
		if (prependNotPostpend) {
			try {
				StringBuffer origText = new StringBuffer();
				BufferedReader br = new BufferedReader(new FileReader(target));
				String line = br.readLine();
				while (line != null) {
					origText.append(line);
					origText.append("\n");
					line = br.readLine();
				}
				br.close();
				FileOutputStream fos = new FileOutputStream(target, false);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
				bw.write(text);
				bw.write(origText.toString());
				bw.flush();
				bw.close();
				fos.getFD().sync();
			} catch (FileNotFoundException e) {
				LOG.error(e.getMessage());
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
		} else {
			try {
				FileOutputStream fos = new FileOutputStream(target, true);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
				bw.write(text);
				bw.flush();
				bw.close();
				fos.getFD().sync();
			} catch (FileNotFoundException e) {
				LOG.error(e.getMessage());
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
		}
	}

	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		return new CHBenCHmarkLoader(this, conn);
	}
	
}
