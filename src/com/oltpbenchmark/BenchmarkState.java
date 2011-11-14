package com.oltpbenchmark;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.oltpbenchmark.ThreadBench.State;
import com.oltpbenchmark.util.QueueLimitException;

public final class BenchmarkState {
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
	//private LoadLineReader llr;
	private volatile Phase currentPhase;

	public Phase getCurrentPhase() {
		return currentPhase;
	}

	public void setCurrentPhase(Phase currentPhase) {
		this.currentPhase = currentPhase;
	}

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
	public void signalDone() {
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
			//this.llr = new LoadLineReader(llr);
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