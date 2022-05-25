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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WikipediaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(WikipediaUtil.class);

    private final FlatHistogram<Integer> namespaceHistogram;
    private final Map<Integer, Random> namespaceRandomMap = new HashMap<>();
    private final Map<Integer, AtomicLong> newNamespaceCounterMap = new HashMap<>();
    private final Map<Integer, AtomicLong> existingNamespaceCounterMap = new HashMap<>();

    private final Lock lock = new ReentrantLock();


    public WikipediaUtil(Random random) {
        namespaceHistogram = new FlatHistogram<>(random, PageHistograms.NAMESPACE);

        for (Integer namespace : PageHistograms.NAMESPACE.values()) {
            newNamespaceCounterMap.put(namespace, new AtomicLong(0));
            existingNamespaceCounterMap.put(namespace, new AtomicLong(0));
            namespaceRandomMap.put(namespace, new Random(namespace));
        }
    }

    // there are a max number of pages distributed across a limited number of namespaces.  i need to track this distribution and then replay.

    public String createNewPageTitle(int namespace) {

        long newCount = newNamespaceCounterMap.get(namespace).incrementAndGet();

        LOG.debug("new count for namespace [{}] is count [{}]", namespace, newCount);

        Random random = namespaceRandomMap.get(namespace);
        return TextGenerator.randomStr(random, 100);
    }

    public String getExistingPageTitle(int namespace) {

        long maxPagesPerNamespace = newNamespaceCounterMap.get(namespace).get();

        if (maxPagesPerNamespace == 0) {
            return null;
        }

        long newCount = existingNamespaceCounterMap.get(namespace).incrementAndGet();

        Random random = namespaceRandomMap.get(namespace);

        if (newCount % maxPagesPerNamespace == 0) {
            lock.lock();
            try {
                LOG.debug("for namespace [{}] maxPages is [{}] and new call count is [{}]; reseeding", namespace, maxPagesPerNamespace, newCount);
                random.setSeed(namespace);
                existingNamespaceCounterMap.get(namespace).set(0);
            } finally {
                lock.unlock();
            }
        }

        return TextGenerator.randomStr(random, 100);
    }

    public int getNamespace() {
        return namespaceHistogram.nextInt();
    }


}
