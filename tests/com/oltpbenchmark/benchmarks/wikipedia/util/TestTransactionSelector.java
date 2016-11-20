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

package com.oltpbenchmark.benchmarks.wikipedia.util;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.TextGenerator;

import junit.framework.TestCase;

public class TestTransactionSelector extends TestCase {

    private static final int NUM_TRACES = 1000;
    private static final int NUM_USERS = 10000;
    private static final int NUM_NAMESPACES = 100;
    
    private Random rng = new Random();
    private File traceFile;
    
    @Override
    protected void setUp() throws Exception {
        this.traceFile = FileUtil.getTempFile("trace", true);
    }
    
    private void populateTraceFile(File f, boolean appendHyphen) throws Exception {
        // Generate a bunch of fake traces
        PrintStream ps = new PrintStream(f);
        for (int i = 0; i < NUM_TRACES; i++) {
            int userId = rng.nextInt(NUM_USERS-1)+1;
            int pageNamespace = rng.nextInt(NUM_NAMESPACES);
            // Use special barriers to make sure that we get everything
            String title = TextGenerator.randomStr(rng, rng.nextInt(100)) + "<<";
            if (appendHyphen) title += " - ";
            TransactionSelector.writeEntry(ps, userId, pageNamespace, title);
        } // FOR
        ps.close();
    }
    
    private void validate(List<WikipediaOperation> ops) {
        assertNotNull(ops);
        assertEquals(NUM_TRACES, ops.size());
        
        for (WikipediaOperation o : ops) {
            assertNotNull(o);
            assertTrue(o.toString(), o.userId > 0);
            assertNotNull(o.toString(), o.pageTitle);
            assertTrue(o.toString(), o.pageTitle.endsWith("<<"));
        } // FOR
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.traceFile.exists()) this.traceFile.delete();
    }
    
    /**
     * testSynthetic
     */
    public void testSynthetic() throws Exception {
        this.populateTraceFile(this.traceFile, false);
        TransactionSelector ts = new TransactionSelector(this.traceFile, null);
        this.validate(ts.readAll());
    }
    
    /**
     * testReal
     */
    public void testReal() throws Exception {
        this.populateTraceFile(this.traceFile, true);
        TransactionSelector ts = new TransactionSelector(this.traceFile, null);
        this.validate(ts.readAll());
    }
    
}
