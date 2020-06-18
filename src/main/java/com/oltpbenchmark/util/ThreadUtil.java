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
package com.oltpbenchmark.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public abstract class ThreadUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadUtil.class);

    public static int availableProcessors() {
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }


    /**
     * @param <R>
     * @param threads
     */
    public static <R extends Runnable> void runNewPool(final Collection<R> threads, int max_concurrent) {
        ExecutorService pool = Executors.newFixedThreadPool(max_concurrent, factory);
        ThreadUtil.run(threads, pool);
    }

    /**
     * For a given list of threads, execute them all (up to max_concurrent at a
     * time) and return once they have completed. If max_concurrent is null,
     * then all threads will be fired off at the same time
     *
     * @param runnables
     * @param pool
     * @throws Exception
     */
    private static <R extends Runnable> void run(final Collection<R> runnables, final ExecutorService pool) {
        final long start = System.currentTimeMillis();
        final int num_threads = runnables.size();
        final CountDownLatch latch = new CountDownLatch(num_threads);
        LatchedExceptionHandler handler = new LatchedExceptionHandler(latch);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Executing %d threads and blocking until they finish", num_threads));
        }
        for (R r : runnables) {
            pool.execute(new LatchRunnable(r, latch, handler));
        }

        pool.shutdown();


        try {
            latch.await();
        } catch (InterruptedException ex) {
            LOG.error("ThreadUtil.run() was interrupted!", ex);
            throw new RuntimeException(ex);
        } finally {
            if (handler.hasError()) {
                String msg = "Failed to execute threads: " + handler.getLastError().getMessage();
                throw new RuntimeException(msg, handler.getLastError());
            }
        }
        if (LOG.isDebugEnabled()) {
            final long stop = System.currentTimeMillis();
            LOG.debug(String.format("Finished executing %d threads [time=%.02fs]",
                    num_threads, (stop - start) / 1000d));
        }
    }

    private static final ThreadFactory factory = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return (t);
        }
    };

    private static class LatchRunnable implements Runnable {
        private final Runnable r;
        private final CountDownLatch latch;
        private final Thread.UncaughtExceptionHandler handler;

        public LatchRunnable(Runnable r, CountDownLatch latch, Thread.UncaughtExceptionHandler handler) {
            this.r = r;
            this.latch = latch;
            this.handler = handler;
        }

        @Override
        public void run() {
            Thread.currentThread().setUncaughtExceptionHandler(this.handler);
            this.r.run();
            this.latch.countDown();
        }
    }

}
