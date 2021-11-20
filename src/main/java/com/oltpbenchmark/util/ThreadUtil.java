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

import com.oltpbenchmark.api.LoaderThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public abstract class ThreadUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadUtil.class);

    public static int availableProcessors() {
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }


    /**
     * For a given list of threads, execute them all (up to max_concurrent at a
     * time) and return once they have completed. If max_concurrent is null,
     * then all threads will be fired off at the same time
     *
     * @param loaderThreads
     * @param maxConcurrent
     * @throws Exception
     */
    public static void runLoaderThreads(final Collection<LoaderThread> loaderThreads, int maxConcurrent) throws InterruptedException {

        final int loaderThreadSize = loaderThreads.size();

        int poolSize =  Math.max(1, Math.min(maxConcurrent, loaderThreadSize));

        int threadOverflow = (loaderThreadSize > poolSize ? loaderThreadSize - poolSize : 0);

        if (LOG.isInfoEnabled()) {
            LOG.info("Creating a Thread Pool with a size of {} to run {} Loader Threads.  {} threads will be queued.", poolSize, loaderThreadSize, threadOverflow);
        }

        ExecutorService service = Executors.newFixedThreadPool(poolSize, factory);

        final long start = System.currentTimeMillis();

        final CountDownLatch latch = new CountDownLatch(loaderThreadSize);

        try {
            for (LoaderThread loaderThread : loaderThreads) {
                service.execute(new LatchRunnable(loaderThread, latch));
            }

            LOG.trace("All Loader Threads executed; waiting on latches...");
            latch.await();

        } finally {

            LOG.trace("Attempting to shutdown the pool...");

            service.shutdown();

            boolean cleanTermination = service.awaitTermination(5, TimeUnit.MINUTES);

            if (cleanTermination) {
                LOG.trace("Pool shut down cleanly!");
            } else {
                LOG.warn("Pool shut down after termination timeout expired.  Likely caused by an unhandled exception in a Loader Thread causing latch count down.  Will force shutdown now.");

                List<Runnable> notStarted = service.shutdownNow();

                LOG.warn("{} Loader Threads were terminated before starting.", notStarted.size());
            }

            if (LOG.isInfoEnabled()) {
                final long stop = System.currentTimeMillis();
                LOG.info(String.format("Finished executing %d Loader Threads [time=%.02fs]", loaderThreadSize, (stop - start) / 1000d));
            }
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
        private final LoaderThread loaderThread;
        private final CountDownLatch latch;

        public LatchRunnable(LoaderThread loaderThread, CountDownLatch latch) {
            this.loaderThread = loaderThread;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                this.loaderThread.run();
            } catch (Exception e) {
                LOG.error(String.format("Exception in Loader Thread with message: [%s]; will count down latch with count %d and then exit :(", e.getMessage(), this.latch.getCount()), e);
                System.exit(1);
            } finally {
                this.latch.countDown();
            }
        }
    }

}
