/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
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
	
    protected final FlatHistogram<Integer> commentLength;
    protected final FlatHistogram<Integer> minorEdit;
    protected final FlatHistogram<Integer> revisionDelta;
	
	private final File traceInput;
	private final File traceOutput;
	private final int traceSize;
	
	public WikipediaBenchmark(WorkloadConfiguration workConf) {		
		super("wikipedia", workConf, true);
		
		XMLConfiguration xml = workConf.getXmlConfig();
		this.traceInput = (xml != null && xml.containsKey("tracefile") ? new File(xml.getString("tracefile")) : null);
		if (xml != null && xml.containsKey("traceOut")) {
		    this.traceSize = xml.getInt("traceOut");
		    this.traceOutput = new File("wikipedia-" + this.traceSize + "k.trace");
		} else {
		    this.traceSize = 0;
		    this.traceOutput = null;
		}
		
		this.commentLength = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.COMMENT_LENGTH);
		this.minorEdit = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.MINOR_EDIT);
		this.revisionDelta = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.REVISION_DELTA);
	}

	public File getTraceInput() {
	    return (this.traceInput);
	}
	public File getTraceOutput() {
	    return (this.traceOutput);
	}
	public int getTraceSize() {
	    return (this.traceSize);
	}
	
	/**
	 * 
	 * @param orig_text
	 * @return
	 */
	protected char[] generateRevisionText(char orig_text[]) {
	    // Figure out how much we are going to change
        // If the delta is greater than the length of the original
        // text, then we will just cut our length in half. Where is your god now?
        // There is probably some sort of minimal size that we should adhere to, but
        // it's 12:30am and I simply don't feel like dealing with that now
        int delta = revisionDelta.nextValue().intValue();
        if (orig_text.length + delta <= 0) {
            delta = -1 * (orig_text.length / 2);
            if (Math.abs(delta) == orig_text.length) delta = 1;
        }
        orig_text = TextGenerator.resizeText(rng(), orig_text, delta);
        
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
		TransactionSelector transSel = new TransactionSelector(this.traceInput, workConf.getTransTypes());
		List<WikipediaOperation> trace = Collections.unmodifiableList(transSel.readAll());
		
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
