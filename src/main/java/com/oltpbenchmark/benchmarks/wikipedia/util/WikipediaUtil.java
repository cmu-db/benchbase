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

package com.oltpbenchmark.benchmarks.wikipedia.util;

import com.oltpbenchmark.benchmarks.wikipedia.data.PageHistograms;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.TextGenerator;

import java.util.Random;

public abstract class WikipediaUtil {

    public static String generatePageTitle(Random rand, int page_id) {
        // Yo we need to do this to ensure that for a given page_id, we always get back the same title.
        // This is a hack for now (as it will break the <randomSeed> option in the config file.
        // But from what I can tell it works.
        rand.setSeed(page_id);
        FlatHistogram<Integer> h_titleLength = new FlatHistogram<>(rand, PageHistograms.TITLE_LENGTH);
        // HACK: Always append the page id to the title
        // so that it's guaranteed to be unique.
        // Otherwise we can get collisions with larger scale factors.
        int titleLength = h_titleLength.nextValue();
        return TextGenerator.randomStr(rand, titleLength) + " [" + page_id + "]";
    }

    public static int generatePageNamespace(Random rand, int page_id) {
        FlatHistogram<Integer> h_namespace = new FlatHistogram<>(rand, PageHistograms.NAMESPACE);
        return h_namespace.nextInt();
    }
}
