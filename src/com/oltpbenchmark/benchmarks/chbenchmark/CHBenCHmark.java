/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.chbenchmark;


import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q15;
import com.oltpbenchmark.util.ClassUtil;

public class CHBenCHmark extends BenchmarkModule {
	private static final Logger LOG = Logger.getLogger(CHBenCHmark.class);
	
	public CHBenCHmark(WorkloadConfiguration workConf) {
		super("chbenchmark", workConf, true);
	}
	
	protected Package getProcedurePackageImpl() {
		return (Q15.class.getPackage());
	}
	
	/**
	 * @param Bool
	 */
	@Override
	protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
        // Drop any existing views from past benchmark executions
        tryDropOldViews();

		// HACK: Turn off terminal messages
		List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();

		int numTerminals = workConf.getTerminals();
        LOG.debug(String.format("Creating %d workers for CHBenCHMark", numTerminals));
        for (int i = 0; i < numTerminals; i++)
            workers.add(new CHBenCHmarkWorker(this, i));

		return workers;
	}
	
	protected Loader<CHBenCHmark> makeLoaderImpl() throws SQLException {
		return new CHBenCHmarkLoader(this);
	}

    private void tryDropOldViews() {
        Q15 query = (Q15)ClassUtil.newInstance(Q15.class, new Object[0], new Class<?>[0]);
        Connection conn = null;
        try {
            conn = makeConnection();
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(query.dropview_stmt.getSQL());
        } catch (SQLException ex) {
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                }
            }
        }
    }
	
}
