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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.wikipedia.data.RevisionHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.procedures.AddWatchList;
import com.oltpbenchmark.benchmarks.wikipedia.util.TraceTransactionGenerator;
import com.oltpbenchmark.benchmarks.wikipedia.util.TransactionSelector;
import com.oltpbenchmark.benchmarks.wikipedia.util.WikipediaOperation;
import com.oltpbenchmark.util.TextGenerator;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;

public class WikipediaBenchmark extends BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(WikipediaBenchmark.class);
    
    protected final FlatHistogram<Integer> commentLength;
    protected final FlatHistogram<Integer> minorEdit;
    private final FlatHistogram<Integer> revisionDeltas[];
	
	private final File traceInput;
	private final File traceOutput;
	private final File traceOutputDebug;
	private final int traceSize;
	
	@SuppressWarnings("unchecked")
    public WikipediaBenchmark(WorkloadConfiguration workConf) {		
		super("wikipedia", workConf, true);
		
		XMLConfiguration xml = workConf.getXmlConfig();
		if (xml != null && xml.containsKey("tracefile")) {
		    this.traceInput = new File(xml.getString("tracefile"));
        } else {
            this.traceInput = null;
        }
		if (xml != null && xml.containsKey("traceOut")) {
		    this.traceSize = xml.getInt("traceOut");
		} else {
		    this.traceSize = 0;
		}
		if (xml != null && xml.containsKey("tracefile")) {
            this.traceOutput = new File(xml.getString("tracefile"));		    
		} else {
		    this.traceOutput = null;
		}
		if (xml != null && xml.containsKey("tracefiledebug")) {
            this.traceOutputDebug = new File(xml.getString("tracefiledebug"));
        } else {
            this.traceOutputDebug = null;
        }
		
		this.commentLength = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.COMMENT_LENGTH);
		this.minorEdit = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.MINOR_EDIT);
		this.revisionDeltas = (FlatHistogram<Integer>[])new FlatHistogram[RevisionHistograms.REVISION_DELTA_SIZES.length];
		for (int i = 0; i < this.revisionDeltas.length; i++) {
		    this.revisionDeltas[i] = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.REVISION_DELTAS[i]);
		} // FOR
	}

	public File getTraceInput() {
	    return (this.traceInput);
	}
	public File getTraceOutput() {
	    return (this.traceOutput);
	}
	public File getTraceOutputDebug() {
	    return (this.traceOutputDebug);
	}
	
	public int getTraceSize() {
	    return (this.traceSize);
	}
	
	/**
	 * Special function that takes in a char field that represents the last
	 * version of the page and then do some permutation on it. This ensures
	 * that each revision looks somewhat similar to previous one so that we
	 * just don't have a bunch of random text fields for the same page.
	 * @param orig_text
	 * @return
	 */
	protected char[] generateRevisionText(char orig_text[]) {
	    // Figure out how much we are going to change
        // If the delta is greater than the length of the original
        // text, then we will just cut our length in half. Where is your god now?
        // There is probably some sort of minimal size that we should adhere to, but
        // it's 12:30am and I simply don't feel like dealing with that now
	    FlatHistogram<Integer> h = null;
	    for (int i = 0; i < this.revisionDeltas.length-1; i++) {
	        if (orig_text.length <= RevisionHistograms.REVISION_DELTA_SIZES[i]) {
	            h = this.revisionDeltas[i];
	        }
	    } // FOR
	    if (h == null) h = this.revisionDeltas[this.revisionDeltas.length-1];
	    assert(h != null);
	    
        int delta = h.nextValue().intValue();
        if (orig_text.length + delta <= 0) {
            delta = -1 * (int)Math.round(orig_text.length / 1.5);
            if (Math.abs(delta) == orig_text.length && delta < 0) delta /= 2;
        }
        if (delta != 0) orig_text = TextGenerator.resizeText(rng(), orig_text, delta);
        
        // And permute it a little bit. This ensures that the text is slightly
        // different than the last revision
        orig_text = TextGenerator.permuteText(rng(), orig_text);
        
        return (orig_text);
	}
	
	@Override
	protected Package getProcedurePackageImpl() {
		return (AddWatchList.class.getPackage());
	}
	
	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
	    LOG.info(String.format("Initializing %d %s using '%s' as the input trace file",
                               workConf.getTerminals(), WikipediaWorker.class.getSimpleName(), this.traceInput));
		TransactionSelector transSel = new TransactionSelector(this.traceInput, workConf.getTransTypes());
		List<WikipediaOperation> trace = Collections.unmodifiableList(transSel.readAll());
		LOG.info("Total Number of Sample Operations: " + trace.size());
		
		ArrayList<Worker> workers = new ArrayList<Worker>();
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			TransactionGenerator<WikipediaOperation> generator = new TraceTransactionGenerator(trace);
			WikipediaWorker worker = new WikipediaWorker(i, this, generator);
			workers.add(worker);
		} // FOR
		return workers;
	}
	
	@Override
	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		return new WikipediaLoader(this, conn);
	}
}
