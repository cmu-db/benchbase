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

import java.util.ArrayList;
import java.util.Iterator;

/** Efficiently stores a record of (start time, latency) pairs. */
public class LatencyRecord implements Iterable<LatencyRecord.Sample> {
	/** Allocate int[] arrays of this length (262144 = 1 MB). */
	static final int BLOCK_SIZE = 262144;

	/**
	 * Contains (start time, latency, transactionType, workerid, phaseid) pentiplets 
	 * in microsecond form. The start times are "compressed" by encoding them as 
	 * increments, starting from startNs. A 32-bit integer provides sufficient resolution
	 * for an interval of 2146 seconds, or 35 minutes.
	 */
	// TODO: Use a real variable length encoding?
	private final ArrayList<long[]> values = new ArrayList<long[]>();
	private int nextIndex;

	private final long startNs;
	private long lastNs;

	public LatencyRecord(long startNs) {
		assert startNs > 0;

		this.startNs = startNs;
		lastNs = startNs;
		allocateChunk();

	}

	public void addLatency(int transType, long startNs, long endNs, int workerId, int phaseId) {
		assert lastNs > 0;
		assert lastNs - 500 <= startNs;
		assert endNs >= startNs;

		if (nextIndex >= BLOCK_SIZE - 5) { // barzan: I changed this!
			allocateChunk();
		}
		long[] chunk = values.get(values.size() - 1);

		long startOffsetUs = ((startNs - lastNs + 500) / 1000);
		assert startOffsetUs >= 0;
		int latencyUs = (int) ((endNs - startNs + 500) / 1000);
		assert latencyUs >= 0;

		chunk[nextIndex] = transType;
		chunk[nextIndex + 1] = startOffsetUs;
		chunk[nextIndex + 2] = latencyUs;
		chunk[nextIndex + 3] = workerId;
		chunk[nextIndex + 4] = phaseId;
		nextIndex += 5;

		lastNs += startOffsetUs * 1000L;
	}

	private void allocateChunk() {
		assert (values.isEmpty() && nextIndex == 0)
				|| nextIndex >= BLOCK_SIZE - 5;
		values.add(new long[BLOCK_SIZE]);
		nextIndex = 0;
	}

	/** Returns the number of recorded samples. */
	public int size() {
		// Samples stored in full chunks
		int samples = (values.size() - 1) * (BLOCK_SIZE / 5);

		// Samples stored in the last not full chunk
		samples += nextIndex / 5;
		return samples;
	}

	/** Stores the start time and latency for a single sample. Immutable. */
	public static final class Sample implements Comparable<Sample> {
		public final int tranType;
		public final long startNs;
		public final int latencyUs;
		public final int workerId;
		public final int phaseId;

		public Sample(int tranType, long startNs, int latencyUs, int workerId, int phaseId) {
			this.tranType = tranType;
			this.startNs = startNs;
			this.latencyUs = latencyUs;
			this.workerId = workerId;
			this.phaseId = phaseId;
		}

		@Override
		public int compareTo(Sample other) {
			long diff = this.startNs - other.startNs;

			// explicit comparison to avoid long to int overflow
			if (diff > 0)
				return 1;
			else if (diff < 0)
				return -1;
			else {
				assert diff == 0;
				return 0;
			}
		}
	}

	private final class LatencyRecordIterator implements Iterator<Sample> {
		private int chunkIndex = 0;
		private int subIndex = 0;
		private long lastIteratorNs = startNs;

		@Override
		public boolean hasNext() {
			if (chunkIndex < values.size() - 1) {
				return true;
			}

			assert chunkIndex == values.size() - 1;
			if (subIndex < nextIndex) {
				return true;
			}

			assert chunkIndex == values.size() - 1 && subIndex == nextIndex;
			return false;
		}

		@Override
		public Sample next() {
			long[] chunk = values.get(chunkIndex);
			int tranType = (int) chunk[subIndex];
			long offsetUs = chunk[subIndex + 1];
			int latencyUs = (int) chunk[subIndex + 2];
			int workerId = (int) chunk[subIndex + 3];
			int phaseId = (int) chunk[subIndex + 4];
			subIndex += 5;
			if (subIndex >= BLOCK_SIZE - 5) {
				chunkIndex += 1;
				subIndex = 0;
			}

			long startNs = lastIteratorNs + offsetUs * 1000L;
			lastIteratorNs = startNs;
			return new Sample(tranType, startNs, latencyUs, workerId, phaseId);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove is not supported");
		}
	}

	public Iterator<Sample> iterator() {
		return new LatencyRecordIterator();
	}
}
