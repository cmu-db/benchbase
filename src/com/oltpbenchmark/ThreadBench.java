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

import com.oltpbenchmark.LatencyRecord.Sample;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.QueueLimitException;


public class ThreadBench implements Thread.UncaughtExceptionHandler{
	private BenchmarkState testState;
	private final List<? extends Worker> workers;
	private File profileFile;
	private static WorkloadConfiguration workConf;
	ArrayList<LatencyRecord.Sample> samples = new ArrayList<LatencyRecord.Sample>();
	
	private ThreadBench(List<? extends Worker> workers) {
		this.workers = workers;
	}

	public ThreadBench(List<? extends Worker> workers, File profileFile) {
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
			out.println("time(sec), throughput(req/sec), avg_lat(ms), min_lat(ms), 25th_lat(ms), median_lat(ms), 75th_lat(ms), 90th_lat(ms), 95th_lat(ms), 99th_lat(ms), max_lat(ms)");
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


	private ArrayList<Thread> createWorkerThreads(boolean isRateLimited) {
		assert testState == null;
		testState = new BenchmarkState(workers.size() + 1, isRateLimited, RATE_QUEUE_LIMIT);
		ArrayList<Thread> workerThreads = new ArrayList<Thread>(workers.size());
		for (Worker worker : workers) {
			worker.setBenchmark(testState);
			Thread thread = new Thread(worker);
			thread.setUncaughtExceptionHandler(this);
			thread.start();
			workerThreads.add(thread);
		}
		return workerThreads;
	}

	private static final int RATE_QUEUE_LIMIT = 10000;


	private int waitForThreadExit(ArrayList<Thread> workerThreads)
			throws InterruptedException {
		assert testState.getState() == State.DONE
				|| testState.getState() == State.EXIT;
		int requests = 0;
		for (int i = 0; i < workerThreads.size(); ++i) {
			
			//FIXME not sure this is the best solution... ensure we don't hang forever, however we might ignore 
			//problems
			workerThreads.get(i).join(60000);  //wait for 60second for threads to terminate... hands otherwise
			
			
			/*// CARLO: Maybe we might want to do this to kill threads that are hanging... 
			  if (workerThreads.get(i).isAlive()) {
				workerThreads.get(i).kill();
				try {
					workerThreads.get(i).join();
				}
				catch (InterruptedException e) {
				}
			}*/
			
			
			requests += workers.get(i).getRequests();
		}
		testState = null;
		return requests;
	}



	/*
	public static Results runRateLimitedBenchmark(List<Worker> workers,
			File profileFile) throws QueueLimitException, IOException {
		ThreadBench bench = new ThreadBench(workers, profileFile);
		return bench.runRateLimitedFromFile();
	}
	*/

	public static Results runRateLimitedBenchmark(List<Worker> workers)
			throws QueueLimitException, IOException {
		ThreadBench bench = new ThreadBench(workers);
		return bench.runRateLimitedMultiPhase();
	}


	public Results runRateLimitedMultiPhase() throws QueueLimitException,
			IOException {

		ArrayList<Thread> workerThreads = createWorkerThreads(true);
		testState.blockForStart();

		// long measureStart = start;

		long start = System.nanoTime();
		long measureEnd = -1;

		Phase phase = workConf.getNextPhase();
		testState.setCurrentPhase(phase);
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

				// Fetch a new Phase
				phase = workConf.getNextPhase();
				testState.setCurrentPhase(phase);
				if (phase == null) {
					// Last phase
					lastEntry = true;
				} else {
					delta += phase.time * 1000000000L;
					System.out.println("[Starting Phase] [Time= " + phase.time
							+ "] [Rate= " + phase.rate + "] [Ratios= "
							+ phase.weights + "]");
					// update frequency in which we check according to wakeup speed
					intervalNs = (long) (1000000000. / (double) phase.rate + 0.5); 
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

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		//HERE WE HANDLE THE CASE IN WHICH ONE OF OUR WOKERTHREADS DIED
		e.printStackTrace();
		System.exit(-1);
		
		/*
		 * Alternatively, we could keep an HashMap<Thread,Worker> storing the runnable for each thread, 
		 * so that we can get the latency numbers from a thread that died, and either continue 
		 * or at least report current status. (Remember to remove this thread from the list of threads to wait for) 
		 */
		
		
	}

	public static void setWorkConf(WorkloadConfiguration workConfig) {
		// TODO Auto-generated method stub
		workConf= workConfig;
	}	
		
	

			
}
