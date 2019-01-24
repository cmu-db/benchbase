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

package com.oltpbenchmark;

import java.util.StringTokenizer;

public class LoadLineReader {

	
	public long timeInSec = -1;
	public long ratePerSec = -1;
	public double[] transactionRatios = null;
	
	
	public LoadLineReader(String fileLine) {

		assert(fileLine!=null);
		StringTokenizer st = new StringTokenizer(fileLine);
	
		transactionRatios = new double[st.countTokens()-2];
	
		timeInSec = Long.parseLong(st.nextToken());
		ratePerSec = Long.parseLong(st.nextToken());
		if (ratePerSec < 1) {
			ratePerSec = 1;
		}
	
		for(int i =0; i< transactionRatios.length;i++)
			transactionRatios[i]=Double.parseDouble(st.nextToken());
		
	}

	public LoadLineReader(LoadLineReader llr) {

		this.timeInSec = llr.timeInSec;
		this.ratePerSec = llr.ratePerSec;
		this.transactionRatios = llr.transactionRatios.clone();
	
	}

	public double requestsPerSecond() {
		return ratePerSec;
	}

}
