/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.wikipedia;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.wikipedia.data.RevisionHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.procedures.AddWatchList;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.TextGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class WikipediaBenchmark extends BenchmarkModule {
    private static final Logger LOG = LoggerFactory.getLogger(WikipediaBenchmark.class);

    protected final FlatHistogram<Integer> commentLength;
    protected final FlatHistogram<Integer> minorEdit;
    private final FlatHistogram<Integer>[] revisionDeltas;

    protected final int num_users;
    protected final int num_pages;

    @SuppressWarnings("unchecked")
    public WikipediaBenchmark(WorkloadConfiguration workConf) {
        super(workConf);

        this.commentLength = new FlatHistogram<>(this.rng(), RevisionHistograms.COMMENT_LENGTH);
        this.minorEdit = new FlatHistogram<>(this.rng(), RevisionHistograms.MINOR_EDIT);
        this.revisionDeltas = new FlatHistogram[RevisionHistograms.REVISION_DELTA_SIZES.length];
        for (int i = 0; i < this.revisionDeltas.length; i++) {
            this.revisionDeltas[i] = new FlatHistogram<>(this.rng(), RevisionHistograms.REVISION_DELTAS[i]);
        }

        this.num_users = (int) Math.ceil(WikipediaConstants.USERS * this.getWorkloadConfiguration().getScaleFactor());
        this.num_pages = (int) Math.ceil(WikipediaConstants.PAGES * this.getWorkloadConfiguration().getScaleFactor());
    }

    /**
     * Special function that takes in a char field that represents the last
     * version of the page and then do some permutation on it. This ensures that
     * each revision looks somewhat similar to previous one so that we just
     * don't have a bunch of random text fields for the same page.
     *
     * @param orig_text
     * @return
     */
    protected char[] generateRevisionText(char[] orig_text) {
        // Figure out how much we are going to change
        // If the delta is greater than the length of the original
        // text, then we will just cut our length in half.
        // Where is your god now?
        // There is probably some sort of minimal size that we should adhere to,
        // but it's 12:30am and I simply don't feel like dealing with that now
        FlatHistogram<Integer> h = null;
        for (int i = 0; i < this.revisionDeltas.length - 1; i++) {
            if (orig_text.length <= RevisionHistograms.REVISION_DELTA_SIZES[i]) {
                h = this.revisionDeltas[i];
            }
        }
        if (h == null) {
            h = this.revisionDeltas[this.revisionDeltas.length - 1];
        }


        int delta = h.nextValue();
        if (orig_text.length + delta <= 0) {
            delta = -1 * (int) Math.round(orig_text.length / 1.5);
            if (Math.abs(delta) == orig_text.length && delta < 0) {
                delta /= 2;
            }
        }
        if (delta != 0) {
            orig_text = TextGenerator.resizeText(this.rng(), orig_text, delta);
        }

        // And permute it a little bit. This ensures that the text is slightly
        // different than the last revision
        orig_text = TextGenerator.permuteText(this.rng(), orig_text);

        return (orig_text);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return (AddWatchList.class.getPackage());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        LOG.debug(String.format("Initializing %d %s", this.workConf.getTerminals(), WikipediaWorker.class.getSimpleName()));

        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        for (int i = 0; i < this.workConf.getTerminals(); ++i) {
            WikipediaWorker worker = new WikipediaWorker(this, i);
            workers.add(worker);
        }
        return workers;
    }

    @Override
    protected Loader<WikipediaBenchmark> makeLoaderImpl() {
        return new WikipediaLoader(this);
    }
}
