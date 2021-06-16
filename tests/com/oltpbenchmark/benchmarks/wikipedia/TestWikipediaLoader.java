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

package com.oltpbenchmark.benchmarks.wikipedia;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.catalog.Catalog;

public class TestWikipediaLoader extends AbstractTestLoader<WikipediaBenchmark> {

    private static final String IGNORE[] = {
        WikipediaConstants.TABLENAME_IPBLOCKS,
        WikipediaConstants.TABLENAME_LOGGING,
        WikipediaConstants.TABLENAME_PAGE_BACKUP,
        WikipediaConstants.TABLENAME_PAGE_RESTRICTIONS,
        WikipediaConstants.TABLENAME_RECENTCHANGES,
        WikipediaConstants.TABLENAME_REVISION,
        WikipediaConstants.TABLENAME_TEXT,
        WikipediaConstants.TABLENAME_USER_GROUPS,
        WikipediaConstants.TABLENAME_VALUE_BACKUP,
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(WikipediaBenchmark.class, IGNORE, TestWikipediaBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.1);
        
        // For some reason we have to do this for HSQLDB
        Catalog.setSeparator("");
    }

//    public void testHistograms() throws Exception {
//        Collection<Integer> values = RevisionHistograms.REVISION_DELTA.values();
//        Histogram<Integer> new_h = new Histogram<Integer>();
//        for (Integer v : values) {
//            Integer cnt = RevisionHistograms.REVISION_DELTA.get(v);
//            if (Math.abs(v) >= 100000) {
//                int new_v = (int)Math.round(v / 10000.0d) * 10000;
//                new_h.put(new_v, cnt);
//            }
//            else if (Math.abs(v) >= 10000) {
//                int new_v = (int)Math.round(v / 1000.0d) * 1000;
//                new_h.put(new_v, cnt);
//            }
//            else {
//                new_h.put(v, cnt);
//            }
//        }
//        for (Integer v : new_h.values()) {
//            Integer cnt = new_h.get(v);
//            System.err.printf("this.put(%d, %d);\n", v, cnt);
//        }
//    }
    
}
