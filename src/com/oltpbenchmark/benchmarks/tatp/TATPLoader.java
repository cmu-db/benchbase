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


package com.oltpbenchmark.benchmarks.tatp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Loader.LoaderThread;
import com.oltpbenchmark.catalog.*;
import com.oltpbenchmark.util.SQLUtil;

public class TATPLoader extends Loader<TATPBenchmark> {
    private static final Logger LOG = Logger.getLogger(TATPLoader.class);
    
    private final long subscriberSize;
    private final int batchSize = 100; // FIXME

    public TATPLoader(TATPBenchmark benchmark) {
    	super(benchmark);
    	this.subscriberSize = Math.round(TATPConstants.DEFAULT_NUM_SUBSCRIBERS * this.scaleFactor);
        if (LOG.isDebugEnabled()) LOG.debug("CONSTRUCTOR: " + TATPLoader.class.getName());
    }
    
    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();
        final int numLoaders = this.benchmark.getWorkloadConfiguration().getLoaderThreads();
        final long itemsPerThread = Math.max(this.subscriberSize / numLoaders, 1);
        final int numSubThreads = (int) Math.ceil((double) this.subscriberSize / itemsPerThread);
        final CountDownLatch subLatch = new CountDownLatch(numSubThreads);

        // SUBSCRIBER
        for (int i = 0; i < numSubThreads; i++) {
            final long lo = i * itemsPerThread + 1;
            final long hi = Math.min(this.subscriberSize, (i + 1) * itemsPerThread);

            threads.add(new LoaderThread() {
                @Override
                public void load(Connection conn) throws SQLException {
                    genSubscriber(conn, lo, hi);
                    subLatch.countDown();
                }
            });
        }

        // ACCESS_INFO depends on SUBSCRIBER
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    subLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                genAccessInfo(conn);
            }
        });

        // SPECIAL_FACILITY SPE and CALL_FORWARDING CAL
        // SPE depends on SUBSCRIBER, CAL depends on SPE
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    subLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                genSpeAndCal(conn);
            }
        });

        return threads;
    }

    /**
     * Populate Subscriber table per benchmark spec.
     */
    void genSubscriber(Connection conn, long lo, long hi) throws SQLException {
        // Create a prepared statement
        Table catalog_tbl = benchmark.getTableCatalog(TATPConstants.TABLENAME_SUBSCRIBER);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement pstmt = conn.prepareStatement(sql);

        long total = 0;
        int batch = 0;

        for (long s_id = lo; s_id <= hi; s_id++) {
            int col = 0;
            
            pstmt.setLong(++col, s_id);
            pstmt.setString(++col, TATPUtil.padWithZero((Long) s_id));
            
            // BIT_##
            for (int j = 0; j < 10; j++) {
            	pstmt.setByte(++col, TATPUtil.number(0, 1).byteValue());
            }
            // HEX_##
            for (int j = 0; j < 10; j++) {
            	pstmt.setByte(++col, TATPUtil.number(0, 15).byteValue());
            }
            // BYTE2_##
            for (int j = 0; j < 10; j++) {
            	pstmt.setShort(++col, TATPUtil.number(0, 255).shortValue());
            }
            // msc_location + vlr_location
            for (int j = 0; j < 2; j++) {
            	pstmt.setInt(++col, TATPUtil.number(0, Integer.MAX_VALUE).intValue());
            }
            total++;
            pstmt.addBatch();
            
            if (++batch >= TATPConstants.BATCH_SIZE) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %6d / %d", catalog_tbl.getName(), total, subscriberSize));
                int results[] = pstmt.executeBatch();
                conn.commit();
                assert(results != null);
                batch = 0;
            }
        } // WHILE
        if (batch > 0) {
        	if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %6d / %d", catalog_tbl.getName(), total, subscriberSize));
            int results[] = pstmt.executeBatch();
            conn.commit();
            assert(results != null);
        }

        pstmt.close();
    }

    /**
     * Populate Access_Info table per benchmark spec.
     */
    void genAccessInfo(Connection conn) throws SQLException {
    	// Create a prepared statement
        Table catalog_tbl = benchmark.getTableCatalog(TATPConstants.TABLENAME_ACCESS_INFO);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        PreparedStatement pstmt = conn.prepareStatement(sql);
    	
        int s_id = 0;
        int[] arr = { 1, 2, 3, 4 };

        int[] ai_types = TATPUtil.subArr(arr, 1, 4);
        long total = 0;
        int batch = 0;
        while (s_id++ < subscriberSize) {
            for (int ai_type : ai_types) {
            	int col = 0;
            	pstmt.setLong(++col, s_id);
                pstmt.setByte(++col, (byte)ai_type);
                pstmt.setShort(++col, TATPUtil.number(0, 255).shortValue());
        		pstmt.setShort(++col, TATPUtil.number(0, 255).shortValue());
				pstmt.setString(++col, TATPUtil.astring(3, 3));
				pstmt.setString(++col, TATPUtil.astring(5, 5));
				pstmt.addBatch();
				batch++;
                total++;
            } // FOR
            if (batch >= TATPConstants.BATCH_SIZE) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %6d / %d", TATPConstants.TABLENAME_ACCESS_INFO, total, ai_types.length * subscriberSize));
                int results[] = pstmt.executeBatch();
                assert(results != null);
                conn.commit();
                batch = 0;
            }
        } // WHILE
        if (batch > 0) {
        	if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %6d / %d", TATPConstants.TABLENAME_ACCESS_INFO, total, ai_types.length * subscriberSize));
            int results[] = pstmt.executeBatch();
            assert(results != null);
            conn.commit();
        }
        pstmt.close();
    }

    /**
     * Populate Special_Facility table and CallForwarding table per benchmark
     * spec.
     */
    void genSpeAndCal(Connection conn) throws SQLException {
    	// Create a prepared statement
        Table catalog_spe = benchmark.getTableCatalog(TATPConstants.TABLENAME_SPECIAL_FACILITY);
        Table catalog_cal = benchmark.getTableCatalog(TATPConstants.TABLENAME_CALL_FORWARDING);
        String spe_sql = SQLUtil.getInsertSQL(catalog_spe, this.getDatabaseType());
        PreparedStatement spe_pstmt = conn.prepareStatement(spe_sql);
        int spe_batch = 0;
        long spe_total = 0;
        
        String cal_sql = SQLUtil.getInsertSQL(catalog_cal, this.getDatabaseType());
        PreparedStatement cal_pstmt = conn.prepareStatement(cal_sql);
        long cal_total = 0;
        
        int s_id = 0;
        int[] spe_arr = { 1, 2, 3, 4 };
        int[] cal_arr = { 0, 8, 6 };
        if (LOG.isDebugEnabled()) LOG.debug("subscriberSize = " + subscriberSize);
        if (LOG.isDebugEnabled()) LOG.debug("batchSize = " + TATPConstants.BATCH_SIZE);
        while (s_id++ < subscriberSize) {
            int[] sf_types = TATPUtil.subArr(spe_arr, 1, 4);
            for (int sf_type : sf_types) {
            	int spe_col = 0;
            	spe_pstmt.setLong(++spe_col, s_id);
            	spe_pstmt.setByte(++spe_col, (byte)sf_type);
            	spe_pstmt.setByte(++spe_col, TATPUtil.isActive());
            	spe_pstmt.setShort(++spe_col, TATPUtil.number(0, 255).shortValue());
            	spe_pstmt.setShort(++spe_col, TATPUtil.number(0, 255).shortValue());
            	spe_pstmt.setString(++spe_col, TATPUtil.astring(5, 5));
            	spe_pstmt.addBatch();
            	spe_batch++;
                spe_total++;

                // now call_forwarding
                int[] start_times = TATPUtil.subArr(cal_arr, 0, 3);
                for (int start_time : start_times) {
                	int cal_col = 0;
                	cal_pstmt.setLong(++cal_col, s_id);
                	cal_pstmt.setByte(++cal_col, (byte)sf_type);
                	cal_pstmt.setByte(++cal_col, (byte)start_time);
                	cal_pstmt.setByte(++cal_col, (byte)(start_time + TATPUtil.number(1, 8)));
                	cal_pstmt.setString(++cal_col, TATPUtil.nstring(15, 15));
                	cal_pstmt.addBatch();
                    cal_total++;
                } // FOR
            } // FOR
            
            if (spe_batch > TATPConstants.BATCH_SIZE) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %d (%s %d / %d)",
													TATPConstants.TABLENAME_SPECIAL_FACILITY, spe_total,
													TATPConstants.TABLENAME_SUBSCRIBER, s_id, subscriberSize));
                int results[] = spe_pstmt.executeBatch();
                assert(results != null);
                
                
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %d (%s %d / %d)",
                									TATPConstants.TABLENAME_CALL_FORWARDING, cal_total,
                									TATPConstants.TABLENAME_SUBSCRIBER, s_id, subscriberSize));
                results = cal_pstmt.executeBatch();
                assert(results != null);
                
                spe_batch = 0;
                conn.commit();
            }
        } // WHILE
        LOG.debug("spe_batch = " + spe_batch);
        if (spe_batch > 0) {
            if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %d", TATPConstants.TABLENAME_SPECIAL_FACILITY, spe_total));
            int results[] = spe_pstmt.executeBatch();
            assert(results != null);
            
            if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %d", TATPConstants.TABLENAME_CALL_FORWARDING, cal_total));
            results = cal_pstmt.executeBatch();
            assert(results != null);
            
            conn.commit();
        }
        cal_pstmt.close();
        spe_pstmt.close();
    }
}
