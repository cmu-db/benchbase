/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.auctionmark;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.CloseAuctions;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.GetItem;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.LoadConfig;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.ResetDatabase;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.RandomGenerator;

public class AuctionMarkBenchmark extends BenchmarkModule {

    private final RandomGenerator rng = new RandomGenerator((int)System.currentTimeMillis());
    
	public AuctionMarkBenchmark(WorkloadConfiguration workConf) {
		super("auctionmark", workConf, true);
		
		this.registerSupplementalProcedure(LoadConfig.class);
		this.registerSupplementalProcedure(CloseAuctions.class);
		this.registerSupplementalProcedure(ResetDatabase.class);
	}
	
	public File getDataDir() {
	    URL url = AuctionMarkBenchmark.class.getResource("data");
	    if (url != null) {
	        return new File(url.getPath());
	    }
	    return (null);
	}
	
	public RandomGenerator getRandomGenerator() {
	    return (this.rng);
	}
	
	@Override
	protected Package getProcedurePackageImpl() {
		return (GetItem.class.getPackage());
	}

	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		List<Worker> workers = new ArrayList<Worker>();
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			workers.add(new AuctionMarkWorker(i, this));
		} // FOR
		return (workers);
	}
	
	@Override
	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		return new AuctionMarkLoader(this, conn);
	}
	
	/**
	 * Return the path of the CSV file that has data for the given Table catalog handle
	 * @param data_dir
	 * @param catalog_tbl
	 * @return
	 */
	public static final File getTableDataFile(File data_dir, Table catalog_tbl) {
	    File f = new File(String.format("%s%stable.%s.csv", data_dir.getAbsolutePath(),
	                                                        File.separator,
	                                                        catalog_tbl.getName().toLowerCase()));
	    if (f.exists() == false) f = new File(f.getAbsolutePath() + ".gz");
	    return (f);
	}
}
