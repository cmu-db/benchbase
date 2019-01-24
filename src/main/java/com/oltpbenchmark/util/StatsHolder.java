/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.util;

public class StatsHolder {

	double[] accumulatedValue;
	int count;

	public StatsHolder(int i) {

		accumulatedValue = new double[i];
		count = 0;

	}

	public void add(double[] t) {
		for (int i = 0; i < t.length; i++) {
			accumulatedValue[i] += t[i];
		}
		count++;
	}

	public void reset() {
		count = 0;
		accumulatedValue = new double[accumulatedValue.length];
	}

	public double[] getAverage() {

		double[] ret = new double[accumulatedValue.length];
		for (int i = 0; i < accumulatedValue.length; i++) {
			ret[i] = accumulatedValue[i] / count;
		}

		return ret;
	}

}
