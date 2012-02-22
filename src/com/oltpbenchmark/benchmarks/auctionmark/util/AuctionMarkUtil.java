/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
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
package com.oltpbenchmark.benchmarks.auctionmark.util;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkProfile;
import com.oltpbenchmark.util.FileUtil;

public abstract class AuctionMarkUtil {
    private static final Logger LOG = Logger.getLogger(AuctionMarkUtil.class);
    
    public static File getDataDirectory() {
        File dataDir = null;
        
        // If we weren't given a path, then we need to look for the tests directory and
        // then walk our way up the tree to get to our benchmark's directory
        try {
            File tests_dir = FileUtil.findDirectory("tests");
            assert(tests_dir != null);
            
            dataDir = new File(tests_dir.getAbsolutePath() + File.separator + "frontend" + File.separator +
                               AuctionMarkProfile.class.getPackage().getName().replace('.', File.separatorChar) +
                               File.separator + "data").getCanonicalFile();
            if (LOG.isDebugEnabled()) LOG.debug("Default data directory path = " + dataDir);
            if (!dataDir.exists()) {
                throw new RuntimeException("The default data directory " + dataDir + " does not exist");
            } else if (!dataDir.isDirectory()) {
                throw new RuntimeException("The default data path " + dataDir + " is not a directory");
            }
        } catch (IOException ex) {
            throw new RuntimeException("Unexpected error", ex);
        }
        return (dataDir);
    }
    
    private static final long ITEM_ID_MASK = 0x0FFFFFFFFFFFFFFFl; 

    /**
     * 
     * @param item_id
     * @param idx
     * @return
     */
    public static long getUniqueElementId(long item_id, int idx) {
        return ((long) idx << 60) | (item_id & ITEM_ID_MASK);
    }

    /**
     * 
     * @param benchmarkTimes
     * @return
     */
    public static Timestamp getProcTimestamp(Timestamp benchmarkTimes[]) {
        assert(benchmarkTimes.length == 2);
        
        Timestamp tmp = new Timestamp(System.currentTimeMillis());
        long timestamp = getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], tmp);
        tmp.setTime(timestamp);
        
        return (tmp);
    }
    
    /**
     * 
     * @param benchmarkStart
     * @param clientStart
     * @param current
     * @return
     */
    public static long getScaledTimestamp(Timestamp benchmarkStart, Timestamp clientStart, Timestamp current) {
        // First get the offset between the benchmarkStart and the clientStart
        // We then subtract that value from the current time. This gives us the total elapsed 
        // time from the current time to the time that the benchmark start (with the gap 
        // from when the benchmark was loading data cut out) 
        long base = benchmarkStart.getTime();
        long offset = current.getTime() - (clientStart.getTime() - base);
        long elapsed = (offset - base) * AuctionMarkConstants.TIME_SCALE_FACTOR;
        return (base + elapsed);
    }
    
}
