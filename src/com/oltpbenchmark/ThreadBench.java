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
package com.oltpbenchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.oltpbenchmark.LatencyRecord.Sample;
import com.oltpbenchmark.WorkLoadConfiguration.Phase;
import com.oltpbenchmark.tpcc.jTPCCConfig;


public class ThreadBench {
	private BenchmarkState testState;
	private final List<? extends Worker> workers;
	private File profileFile;
	private static final WorkLoadConfiguration workConf = WorkLoadConfiguration
			.getInstance();

	private static enum State {
		WARMUP, MEASURE, DONE, EXIT,
	}

	private static final class BenchmarkState {
		private final int queueLimit;

		private volatile State state = State.WARMUP;

		// Assigned a value when starting the test. Used for offsets in the
		// latency record.
		private final long testStartNs;

		public long getTestStartNs() {
			return testStartNs;
		}

		private final CountDownLatch startBarrier;
		private AtomicInteger notDoneCount;

		// Protected by this
		private int workAvailable = 0;
		private int workersWaiting = 0;
		private LoadLineReader llr;
		private Phase currentPhase;

		/**
		 * 
		 * @param numThreads
		 *            number of threads involved in the test: including the
		 *            master thread.
		 * @param rateLimited
		 * @param queueLimit
		 */
		public BenchmarkState(int numThreads, boolean rateLimited,
				int queueLimit) {
			this.queueLimit = queueLimit;
			startBarrier = new CountDownLatch(numThreads);
			notDoneCount = new AtomicInteger(numThreads);

			assert numThreads > 0;
			if (!rateLimited) {
				workAvailable = -1;
			} else {
				assert queueLimit > 0;
			}

			testStartNs = System.nanoTime();
		}

		public State getState() {
			return state;
		}

		/**
		 * Wait for all threads to call this. Returns once all the threads have
		 * entered.
		 */
		public void blockForStart() {
			assert state == State.WARMUP;
			assert startBarrier.getCount() > 0;
			startBarrier.countDown();
			try {
				startBarrier.await();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		public void startMeasure() {
			assert state == State.WARMUP;

			state = State.MEASURE;
		}

		public void startCoolDown() {
			assert state == State.MEASURE;
			state = State.DONE;

			// The master thread must also signal that it is done
			signalDone();
		}

		/** Notify that this thread has entered the done state. */
		private void signalDone() {
			assert state == State.DONE;
			int current = notDoneCount.decrementAndGet();
			assert current >= 0;

			if (current == 0) {
				// We are the last thread to notice that we are done: wake any
				// blocked workers
				state = State.EXIT;
				synchronized (this) {
					if (workersWaiting > 0) {
						this.notifyAll();
					}
				}
			}
		}

		public boolean isRateLimited() {
			// Should be thread-safe due to only being used during
			// initialization
			return workAvailable != -1;
		}

		/**
		 * Add a request to do work.
		 * 
		 * @throws QueueLimitException
		 */
		public void addWork(int amount) throws QueueLimitException {
			assert amount > 0;

			synchronized (this) {
				assert workAvailable >= 0;

				workAvailable += amount;

				if (workAvailable > queueLimit) {
					// TODO: Deal with this appropriately. For now, we are
					// ignoring it.
					workAvailable = queueLimit;
					// throw new QueueLimitException("Work queue limit ("
					// + queueLimit
					// + ") exceeded; Cannot keep up with desired rate");
				}

				if (workersWaiting <= amount) {
					// Wake all waiters
					this.notifyAll();
				} else {
					// Only wake the correct number of waiters
					assert workersWaiting > amount;
					for (int i = 0; i < amount; ++i) {
						this.notify();
					}
				}
				int wakeCount = (workersWaiting < amount) ? workersWaiting
						: amount;
				assert wakeCount <= workersWaiting;
			}
		}

		/** Called by ThreadPoolThreads when waiting for work. */
		public State fetchWork() {
			synchronized (this) {
				if (workAvailable == 0) {
					workersWaiting += 1;
					while (workAvailable == 0) {
						if (state == State.EXIT) {
							return State.EXIT;
						}
						try {
							this.wait();
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
					}
					workersWaiting -= 1;
				}

				assert workAvailable > 0;
				workAvailable -= 1;

				return state;
			}
		}

		/** Called by ThreadPoolThreads when waiting for work. */
		public Phase fetchWorkType() {
			synchronized (this) {
				return currentPhase;
			}
		}

		public void addWork(int amount, boolean resetQueues, LoadLineReader llr)
				throws QueueLimitException {
			assert amount > 0;

			synchronized (this) {
				assert workAvailable >= 0;
				this.llr = new LoadLineReader(llr);
				if (resetQueues)
					workAvailable = amount;
				else
					workAvailable += amount;

				if (workAvailable > queueLimit) {
					// TODO: Deal with this appropriately. For now, we are
					// ignoring it.
					workAvailable = queueLimit;
					// throw new QueueLimitException("Work queue limit ("
					// + queueLimit
					// + ") exceeded; Cannot keep up with desired rate");
				}

				if (workersWaiting <= amount) {
					// Wake all waiters
					this.notifyAll();
				} else {
					// Only wake the correct number of waiters
					assert workersWaiting > amount;
					for (int i = 0; i < amount; ++i) {
						this.notify();
					}
				}
				int wakeCount = (workersWaiting < amount) ? workersWaiting
						: amount;
				assert wakeCount <= workersWaiting;
			}
		}
	}

	public static abstract class Worker implements Runnable {
		private BenchmarkState testState;
		private LatencyRecord latencies;

		@Override
		public final void run() {
			// TODO: Make this an interface; move code to class to prevent reuse
			// In case of reuse reset the measurements
			latencies = new LatencyRecord(testState.getTestStartNs());
			boolean isRateLimited = testState.isRateLimited();

			// wait for start
			testState.blockForStart();

			// System.out.println(this + " start");
			boolean seenDone = false;
			State state = testState.getState();
			while (state != State.EXIT) {
				if (state == State.DONE && !seenDone) {
					// This is the first time we have observed that the test is
					// done
					// notify the global test state, then continue applying load
					seenDone = true;
					testState.signalDone();
				}
				Phase phase = null;
				// apply load
				if (isRateLimited) {
					// re-reads the state because it could have changed if we
					// blocked
					state = testState.fetchWork();
					phase = testState.fetchWorkType();
				}

				boolean measure = state == State.MEASURE;

				// TODO: Measuring latency when not rate limited is ... a little
				// weird because
				// if you add more simultaneous clients, you will increase
				// latency (queue delay)
				// but we do this anyway since it is useful sometimes
				long start = 0;
				if (measure) {
					start = System.nanoTime();
				}

				jTPCCConfig.TransactionType type = doWork(measure, phase);
				if (measure) {
					long end = System.nanoTime();
					latencies.addLatency(type.ordinal(), start, end);
				}
				state = testState.getState();
			}

			tearDown();
			testState = null;
		}

		public int getRequests() {
			return latencies.size();
		}

		public Iterable<LatencyRecord.Sample> getLatencyRecords() {
			return latencies;
		}

		/**
		 * Called in a loop in the thread to exercise the system under test.
		 * 
		 * @param llr
		 */
		protected abstract jTPCCConfig.TransactionType doWork(boolean measure,
				Phase phase);

		/**
		 * Called at the end of the test to do any clean up that may be
		 * required.
		 */
		protected void tearDown() {
		}

		public void setBenchmark(BenchmarkState testState) {
			assert this.testState == null;
			this.testState = testState;
		}
	}

	private ThreadBench(List<? extends Worker> workers) {
		this.workers = workers;
	}

	public ThreadBench(ArrayList<? extends Worker> workers, File profileFile) {
		this.workers = workers;
		this.profileFile = profileFile;
	}

	public static final class TimeBucketIterable implements
			Iterable<DistributionStatistics> {
		private final Iterable<Sample> samples;
		private final int windowSizeSeconds;

		public TimeBucketIterable(Iterable<Sample> samples,
				int windowSizeSeconds) {
			this.samples = samples;
			this.windowSizeSeconds = windowSizeSeconds;
		}

		@Override
		public Iterator<DistributionStatistics> iterator() {
			return new TimeBucketIterator(samples.iterator(), windowSizeSeconds);
		}
	}

	public static final class TimeBucketIterator implements
			Iterator<DistributionStatistics> {
		private final Iterator<Sample> samples;
		private final int windowSizeSeconds;

		private Sample sample;
		private long nextStartNs;

		private DistributionStatistics next;

		public TimeBucketIterator(Iterator<LatencyRecord.Sample> samples,
				int windowSizeSeconds) {
			this.samples = samples;
			this.windowSizeSeconds = windowSizeSeconds;

			if (samples.hasNext()) {
				sample = samples.next();
				// TODO: To be totally correct, we would want this to be the
				// timestamp of the start
				// of the measurement interval. In most cases this won't matter.
				nextStartNs = sample.startNs;
				calculateNext();
			}
		}

		private void calculateNext() {
			assert next == null;
			assert sample != null;
			assert sample.startNs >= nextStartNs;

			// Collect all samples in the time window
			ArrayList<Integer> latencies = new ArrayList<Integer>();
			long endNs = nextStartNs + windowSizeSeconds * 1000000000L;
			while (sample != null && sample.startNs < endNs) {
				latencies.add(sample.latencyUs);

				if (samples.hasNext()) {
					sample = samples.next();
				} else {
					sample = null;
				}
			}

			// Set up the next time window
			assert sample == null || endNs <= sample.startNs;
			nextStartNs = endNs;

			int[] l = new int[latencies.size()];
			for (int i = 0; i < l.length; ++i) {
				l[i] = latencies.get(i);
			}

			next = DistributionStatistics.computeStatistics(l);
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public DistributionStatistics next() {
			if (next == null)
				throw new NoSuchElementException();
			DistributionStatistics out = next;
			next = null;
			if (sample != null) {
				calculateNext();
			}
			return out;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("unsupported");
		}
	}

	public static final class Results {
		public final long nanoSeconds;
		public final int measuredRequests;
		public final DistributionStatistics latencyDistribution;

		public final List<LatencyRecord.Sample> latencySamples;

		public Results(long nanoSeconds, int measuredRequests,
				DistributionStatistics latencyDistribution,
				final List<LatencyRecord.Sample> latencySamples) {
			this.nanoSeconds = nanoSeconds;
			this.measuredRequests = measuredRequests;
			this.latencyDistribution = latencyDistribution;

			if (latencyDistribution == null) {
				assert latencySamples == null;
				this.latencySamples = null;
			} else {
				// defensive copy
				this.latencySamples = Collections
						.unmodifiableList(new ArrayList<LatencyRecord.Sample>(
								latencySamples));
				assert !this.latencySamples.isEmpty();
			}
		}

		public double getRequestsPerSecond() {
			return (double) measuredRequests / (double) nanoSeconds * 1e9;
		}

		@Override
		public String toString() {
			return "Results(nanoSeconds=" + nanoSeconds + ", measuredRequests="
					+ measuredRequests + ") = " + getRequestsPerSecond()
					+ " requests/sec";
		}

		public void writeCSV(int windowSizeSeconds, PrintStream out) {
			out.println("time (seconds),throughput (requests/s),average,min,25th,median,75th,90th,95th,99th,max");
			int i = 0;
			for (DistributionStatistics s : new TimeBucketIterable(
					latencySamples, windowSizeSeconds)) {
				final double MILLISECONDS_FACTOR = 1e3;
				out.printf(
						"%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
						i * windowSizeSeconds, (double) s.getCount()
								/ windowSizeSeconds, s.getAverage()
								/ MILLISECONDS_FACTOR, s.getMinimum()
								/ MILLISECONDS_FACTOR, s.get25thPercentile()
								/ MILLISECONDS_FACTOR, s.getMedian()
								/ MILLISECONDS_FACTOR, s.get75thPercentile()
								/ MILLISECONDS_FACTOR, s.get90thPercentile()
								/ MILLISECONDS_FACTOR, s.get95thPercentile()
								/ MILLISECONDS_FACTOR, s.get99thPercentile()
								/ MILLISECONDS_FACTOR, s.getMaximum()
								/ MILLISECONDS_FACTOR);
				i += 1;
			}
		}

		public void writeAllCSV(PrintStream out) {
			long startNs = latencySamples.get(0).startNs;
			out.println("transaction type (index in config file), start time (microseconds),latency (microseconds)");
			for (Sample s : latencySamples) {
				long startUs = (s.startNs - startNs + 500) / 1000;
				out.println(s.tranType + "," + startUs + "," + s.latencyUs);
			}
		}

		public void writeAllCSVAbsoluteTiming(PrintStream out) {

			// This is needed because nanTime does not guarantee offset... we
			// ground it (and round it) to ms from 1970-01-01 like currentTime
			double x = ((double) System.nanoTime() / (double) 1000000000);
			double y = ((double) System.currentTimeMillis() / (double) 1000);
			double offset = x - y;

			long startNs = latencySamples.get(0).startNs;
			out.println("transaction type (index in config file), start time (microseconds),latency (microseconds)");
			for (Sample s : latencySamples) {
				double startUs = ((double) s.startNs / (double) 1000000000);
				out.println(s.tranType + ","
						+ String.format("%10.6f", startUs - offset) + ","
						+ s.latencyUs);
			}
		}

	}

	public Results runRateLimited(int warmUpSeconds, int measureSeconds,
			int requestsPerSecond) throws QueueLimitException {
		assert requestsPerSecond > 0;

		ArrayList<Thread> workerThreads = createWorkerThreads(true);

		final long intervalNs = (long) (1000000000. / (double) requestsPerSecond + 0.5);

		testState.blockForStart();

		long start = System.nanoTime();
		long measureStart = start + warmUpSeconds * 1000000000L;
		long measureEnd = -1;

		long nextInterval = start + intervalNs;
		int nextToAdd = 1;
		while (true) {
			testState.addWork(nextToAdd);

			// Wait until the interval expires, which may be "don't wait"
			long now = System.nanoTime();
			long diff = nextInterval - now;
			while (diff > 0) { // this can wake early: sleep multiple times to
								// avoid that
				long ms = diff / 1000000;
				diff = diff % 1000000;
				try {
					Thread.sleep(ms, (int) diff);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				now = System.nanoTime();
				diff = nextInterval - now;
			}
			assert diff <= 0;

			// Compute how many messages to deliver
			nextToAdd = (int) (-diff / intervalNs + 1);
			assert nextToAdd > 0;
			nextInterval += intervalNs * nextToAdd;

			// Update the test state appropriately
			State state = testState.getState();
			if (state == State.WARMUP && now >= measureStart) {
				testState.startMeasure();
				measureStart = now;
				measureEnd = measureStart + measureSeconds * 1000000000L;
			} else if (state == State.MEASURE && now >= measureEnd) {
				testState.startCoolDown();
				measureEnd = now;
			} else if (state == State.EXIT) {
				// All threads have noticed the done, meaning all measured
				// requests have definitely finished.
				// Time to quit.
				break;
			}
		}

		try {
			int requests = waitForThreadExit(workerThreads);

			// Combine all the latencies together in the most disgusting way
			// possible: sorting!
			ArrayList<LatencyRecord.Sample> samples = new ArrayList<LatencyRecord.Sample>();
			for (Worker w : workers) {
				for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
					samples.add(sample);
				}
			}
			Collections.sort(samples);

			// Compute stats on all the latencies
			int[] latencies = new int[samples.size()];
			for (int i = 0; i < samples.size(); ++i) {
				latencies[i] = samples.get(i).latencyUs;
			}
			DistributionStatistics stats = DistributionStatistics
					.computeStatistics(latencies);

			return new Results(measureEnd - measureStart, requests, stats,
					samples);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private ArrayList<Thread> createWorkerThreads(boolean isRateLimited) {
		assert testState == null;
		testState = new BenchmarkState(workers.size() + 1, isRateLimited,
				RATE_QUEUE_LIMIT);

		ArrayList<Thread> workerThreads = new ArrayList<Thread>(workers.size());
		for (Worker worker : workers) {
			worker.setBenchmark(testState);
			Thread thread = new Thread(worker);
			thread.start();
			workerThreads.add(thread);
		}
		return workerThreads;
	}

	private static final int RATE_QUEUE_LIMIT = 10000;

	public Results run(int warmUpSeconds, int measureSeconds) {
		ArrayList<Thread> workerThreads = createWorkerThreads(false);

		try {
			testState.blockForStart();
			Thread.sleep(warmUpSeconds * 1000);
			long startNanos = System.nanoTime();

			// Carlo's Version (doesn't work at the moment)

			// testState.startMeasure(startNanos);
			//
			// //output continuously
			// long starttimer = System.currentTimeMillis();
			//
			// while((starttimer + measureSeconds * 1000) >
			// System.currentTimeMillis()){
			// long st = System.currentTimeMillis();
			// Thread.sleep(1000);
			// int currenttps = 0;
			// for(Worker w:workers)
			// currenttps+=w.getRequestsSinceLastCall();
			// long en = System.currentTimeMillis();
			//
			//
			// System.out.println((en-starttimer) +
			// " "+(double)currenttps*(double)1000/(double)(en-st));
			//
			// }
			//
			//
			// Alternative Evan version
			testState.startMeasure();
			Thread.sleep(measureSeconds * 1000);

			testState.startCoolDown();
			long endNanos = System.nanoTime();

			int requests = waitForThreadExit(workerThreads);

			long ns = endNanos - startNanos;
			return new Results(ns, requests, null, null);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private int waitForThreadExit(ArrayList<Thread> workerThreads)
			throws InterruptedException {
		assert testState.getState() == State.DONE
				|| testState.getState() == State.EXIT;
		int requests = 0;
		for (int i = 0; i < workerThreads.size(); ++i) {
			workerThreads.get(i).join();
			requests += workers.get(i).getRequests();
		}
		testState = null;
		return requests;
	}

	public static Results runBenchmark(List<? extends Worker> workers,
			int warmUpSeconds, int measureSeconds) {
		ThreadBench bench = new ThreadBench(workers);
		return bench.run(warmUpSeconds, measureSeconds);
	}

	public static Results runRateLimitedBenchmark(
			List<? extends Worker> workers, int warmUpSeconds,
			int measureSeconds, int requestsPerSecond)
			throws QueueLimitException {
		ThreadBench bench = new ThreadBench(workers);
		return bench.runRateLimited(warmUpSeconds, measureSeconds,
				requestsPerSecond);
	}

	public static Results runRateLimitedBenchmark(ArrayList<Worker> workers,
			File profileFile) throws QueueLimitException, IOException {

		ThreadBench bench = new ThreadBench(workers, profileFile);
		return bench.runRateLimitedFromFile();
	}

	public static Results runRateLimitedBenchmark(ArrayList<Worker> workers)
			throws QueueLimitException, IOException {
		ThreadBench bench = new ThreadBench(workers);
		return bench.runRateLimitedMultiPhase();
	}

	public Results runRateLimitedFromFile() throws QueueLimitException,
			IOException {

		BufferedReader br = new BufferedReader(new FileReader(profileFile));
		ArrayList<Thread> workerThreads = createWorkerThreads(true);
		testState.blockForStart();

		// long measureStart = start;

		long start = System.nanoTime();
		long measureEnd = -1;

		String fileLine = br.readLine();
		LoadLineReader llr = new LoadLineReader(fileLine);
		// System.out.println("Time:" +System.nanoTime()/1000000000+ " Reading:"
		// + fileLine);

		long intervalNs = (long) (1000000000. / (double) llr
				.requestsPerSecond() + 0.5);

		long nextInterval = start + intervalNs;
		int nextToAdd = 1;

		boolean resetQueues = true;
		long delta = llr.timeInSec * 1000000000L;
		boolean lastEntry = false;

		while (true) {

			// posting new work... and reseting the queue in case we have new
			// portion of the workload...
			testState.addWork(nextToAdd, resetQueues, llr);
			resetQueues = false;

			// Wait until the interval expires, which may be "don't wait"
			long now = System.nanoTime();
			long diff = nextInterval - now;
			while (diff > 0) { // this can wake early: sleep multiple times to
								// avoid that
				long ms = diff / 1000000;
				diff = diff % 1000000;
				try {
					Thread.sleep(ms, (int) diff);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				now = System.nanoTime();
				diff = nextInterval - now;
			}
			assert diff <= 0;

			if (start + delta < System.nanoTime() && !lastEntry) { // enters
																	// here
																	// after
																	// each
																	// phase of
																	// the test
				resetQueues = true; // reset the queues so that the new phase is
									// not affected by the queue of the previous
									// one
				// intervalNs = (long) (1000000000. / (double)
				// llr.requestsPerSecond() + 0.5); //update frequency in which
				// we check according to wakeup speed
				fileLine = br.readLine(); // read the next line
				if (fileLine == null || fileLine.equals("")) { // if it is null
																// or empty we
																// are at the
																// end of the
																// file, do not
																// update llr
																// anymore, but
																// set lastEntry
																// to true...
					lastEntry = true;
				} else {
					llr = new LoadLineReader(fileLine);
					delta += llr.timeInSec * 1000000000L;
				}
				intervalNs = (long) (1000000000. / (double) llr
						.requestsPerSecond() + 0.5); // update frequency in
														// which we check
														// according to wakeup
														// speed
			}

			// Compute how many messages to deliver
			nextToAdd = (int) (-diff / intervalNs + 1);
			assert nextToAdd > 0;
			nextInterval += intervalNs * nextToAdd;

			// Update the test state appropriately
			State state = testState.getState();
			if (state == State.WARMUP && now >= start) {
				testState.startMeasure();
				start = now;
				// measureEnd = measureStart + measureSeconds * 1000000000L;
			} else if (state == State.MEASURE && lastEntry
					&& now >= start + delta) {
				testState.startCoolDown();
				measureEnd = now;
			} else if (state == State.EXIT) {
				// All threads have noticed the done, meaning all measured
				// requests have definitely finished.
				// Time to quit.
				break;
			}
		}

		try {
			int requests = waitForThreadExit(workerThreads);

			// Combine all the latencies together in the most disgusting way
			// possible: sorting!
			ArrayList<LatencyRecord.Sample> samples = new ArrayList<LatencyRecord.Sample>();
			for (Worker w : workers) {
				for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
					samples.add(sample);
				}
			}
			Collections.sort(samples);

			// Compute stats on all the latencies
			int[] latencies = new int[samples.size()];
			for (int i = 0; i < samples.size(); ++i) {
				latencies[i] = samples.get(i).latencyUs;
			}
			DistributionStatistics stats = DistributionStatistics
					.computeStatistics(latencies);

			return new Results(measureEnd - start, requests, stats, samples);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public Results runRateLimitedMultiPhase() throws QueueLimitException,
			IOException {

		ArrayList<Thread> workerThreads = createWorkerThreads(true);
		testState.blockForStart();

		// long measureStart = start;

		long start = System.nanoTime();
		long measureEnd = -1;

		Phase phase = workConf.getNextPhase();

		System.out.println("[Starting Phase] [Time= " + phase.time
				+ "] [Rate= " + phase.rate + "] [Ratios= " + phase.weights
				+ "]");

		long intervalNs = (long) (1000000000. / (double) phase.rate + 0.5);

		long nextInterval = start + intervalNs;
		int nextToAdd = 1;

		boolean resetQueues = true;

		long delta = phase.time * 1000000000L;
		boolean lastEntry = false;

		while (true) {

			// posting new work... and reseting the queue in case we have new
			// portion of the workload...

			// testState.addWork(nextToAdd, resetQueues, llr);//FIXME ?

			testState.addWork(nextToAdd);
			resetQueues = false;

			// Wait until the interval expires, which may be "don't wait"
			long now = System.nanoTime();
			long diff = nextInterval - now;
			while (diff > 0) { // this can wake early: sleep multiple times to
								// avoid that
				long ms = diff / 1000000;
				diff = diff % 1000000;
				try {
					Thread.sleep(ms, (int) diff);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				now = System.nanoTime();
				diff = nextInterval - now;
			}
			assert diff <= 0;

			if (start + delta < System.nanoTime() && !lastEntry) {
				// enters here after each phase of the test
				// reset the queues so that the new phase is not affected by the
				// queue of the previous one
				resetQueues = true;

				intervalNs = (long) (1000000000. / (double) phase.rate + 0.5); // update
																				// frequency
																				// in
																				// which
																				// we
																				// check
																				// according
																				// to
																				// wakeup
																				// speed
				phase = workConf.getNextPhase(); // read the next line
				if (phase == null) {
					// Last phase
					lastEntry = true;
				} else {
					delta += phase.time * 1000000000L;
					System.out.println("[Starting Phase] [Time= " + phase.time
							+ "] [Rate= " + phase.rate + "] [Ratios= "
							+ phase.weights + "]");
				}
			}

			// Compute how many messages to deliver
			nextToAdd = (int) (-diff / intervalNs + 1);
			assert nextToAdd > 0;
			nextInterval += intervalNs * nextToAdd;

			// Update the test state appropriately
			State state = testState.getState();
			if (state == State.WARMUP && now >= start) {
				testState.startMeasure();
				start = now;
				// measureEnd = measureStart + measureSeconds * 1000000000L;
			} else if (state == State.MEASURE && lastEntry
					&& now >= start + delta) {
				testState.startCoolDown();
				measureEnd = now;
			} else if (state == State.EXIT) {
				// All threads have noticed the done, meaning all measured
				// requests have definitely finished.
				// Time to quit.
				break;
			}
		}

		try {
			int requests = waitForThreadExit(workerThreads);

			// Combine all the latencies together in the most disgusting way
			// possible: sorting!
			ArrayList<LatencyRecord.Sample> samples = new ArrayList<LatencyRecord.Sample>();
			for (Worker w : workers) {
				for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
					samples.add(sample);
				}
			}
			Collections.sort(samples);

			// Compute stats on all the latencies
			int[] latencies = new int[samples.size()];
			for (int i = 0; i < samples.size(); ++i) {
				latencies[i] = samples.get(i).latencyUs;
			}
			DistributionStatistics stats = DistributionStatistics
					.computeStatistics(latencies);

			return new Results(measureEnd - start, requests, stats, samples);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
