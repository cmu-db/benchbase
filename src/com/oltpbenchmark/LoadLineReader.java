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
