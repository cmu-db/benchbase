package com.oltpbenchmark.benchmarks.chbenchmark;

import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.terminalPrefix;

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
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q1;
import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import com.oltpbenchmark.benchmarks.tpcc.TPCCLoader;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.util.SimpleSystemPrinter;

public class CHBenCHmark extends TPCCBenchmark {
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
		super("chbenchmark", workConf);
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

		CHBenCHmarkWorker[] terminals = new CHBenCHmarkWorker[workConf.getTerminals()];

		int numWarehouses = (int) workConf.getScaleFactor();//tpccConf.getNumWarehouses();
		int numTerminals = workConf.getTerminals();
		assert (numTerminals >= numWarehouses) :
		    String.format("Insufficient number of terminals '%d' [numWarehouses=%d]",
		                  numTerminals, numWarehouses);

		String[] terminalNames = new String[numTerminals];
		// TODO: This is currently broken: fix it!
		int warehouseOffset = Integer.getInteger("warehouseOffset", 1);
		assert warehouseOffset == 1;

		// We distribute terminals evenly across the warehouses
		// Eg. if there are 10 terminals across 7 warehouses, they
		// are distributed as
		// 1, 1, 2, 1, 2, 1, 2
		final double terminalsPerWarehouse = (double) numTerminals
				/ numWarehouses;
		assert terminalsPerWarehouse >= 1;
		for (int w = 0; w < numWarehouses; w++) {
			// Compute the number of terminals in *this* warehouse
			int lowerTerminalId = (int) (w * terminalsPerWarehouse);
			int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);
			// protect against double rounding errors
			int w_id = w + 1;
			if (w_id == numWarehouses)
				upperTerminalId = numTerminals;
			int numWarehouseTerminals = upperTerminalId - lowerTerminalId;

			LOG.info(String.format("w_id %d = %d terminals [lower=%d / upper%d]",
			                       w_id, numWarehouseTerminals, lowerTerminalId, upperTerminalId));

			final double districtsPerTerminal = jTPCCConfig.configDistPerWhse
					/ (double) numWarehouseTerminals;
			assert districtsPerTerminal >= 1 :
			    String.format("Too many terminals [districtsPerTerminal=%.2f, numWarehouseTerminals=%d]",
			                  districtsPerTerminal, numWarehouseTerminals);
			for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
				int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
				int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
				if (terminalId + 1 == numWarehouseTerminals) {
					upperDistrictId = jTPCCConfig.configDistPerWhse;
				}
				lowerDistrictId += 1;

				String terminalName = terminalPrefix + "w" + w_id + "d"
						+ lowerDistrictId + "-" + upperDistrictId;

				CHBenCHmarkWorker terminal = new CHBenCHmarkWorker(terminalName, w_id,
						lowerDistrictId, upperDistrictId, this,
						new SimpleSystemPrinter(null), new SimpleSystemPrinter(
								System.err), numWarehouses);
				terminals[lowerTerminalId + terminalId] = terminal;
				terminalNames[lowerTerminalId + terminalId] = terminalName;
			}

		}
		assert terminals[terminals.length - 1] != null;

		ArrayList<CHBenCHmarkWorker> ret = new ArrayList<CHBenCHmarkWorker>();
		for (CHBenCHmarkWorker w : terminals)
			ret.add(w);
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
