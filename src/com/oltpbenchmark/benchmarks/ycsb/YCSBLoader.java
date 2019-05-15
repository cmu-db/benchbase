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

package com.oltpbenchmark.benchmarks.ycsb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.TextGenerator;

public class YCSBLoader extends Loader<YCSBBenchmark> {
    private static final Logger LOG = Logger.getLogger(YCSBLoader.class);
    private final int num_record;

    public YCSBLoader(YCSBBenchmark benchmark) {
        super(benchmark);
        this.num_record = (int) Math.round(YCSBConstants.RECORD_COUNT * this.scaleFactor);
        if (LOG.isDebugEnabled()) {
            LOG.debug("# of RECORDS:  " + this.num_record);
        }
    }
    
    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();
        int count = 0;
        while (count < this.num_record) {
            final int start = count;
            final int stop = Math.min(start+YCSBConstants.THREAD_BATCH_SIZE, this.num_record);
            threads.add(new LoaderThread() {
                @Override
                public void load(Connection conn) throws SQLException {
                    if (LOG.isDebugEnabled())
                        LOG.debug(String.format("YCSBLoadThread[%d, %d]", start, stop));
                    loadRecords(conn, start, stop);
                }
            });
            count = stop;
        }
        return (threads);
    }

    private void loadRecords(Connection conn, int start, int stop) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog("USERTABLE");
        assert (catalog_tbl != null);
        
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement stmt = conn.prepareStatement(sql);
        long total = 0;
        int batch = 0;
        for (int i = start; i < stop; i++) {
            stmt.setInt(1, i);
            for (int j = 0; j < YCSBConstants.NUM_FIELDS; j++) {
                stmt.setString(j+2, TextGenerator.randomStr(rng(), YCSBConstants.FIELD_SIZE));
            }
            stmt.addBatch();
            total++;
            if (++batch >= YCSBConstants.COMMIT_BATCH_SIZE) {
                int result[] = stmt.executeBatch();
                assert (result != null);
                conn.commit();
                batch = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Records Loaded %d / %d", total, this.num_record));
            }
        } // FOR
        if (batch > 0) {
            stmt.executeBatch();
            conn.commit();
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Records Loaded %d / %d", total, this.num_record));
        }
        stmt.close();
        if (LOG.isDebugEnabled()) LOG.debug("Finished loading " + catalog_tbl.getName());
        return;
    }
    
}
