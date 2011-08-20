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

import java.util.StringTokenizer;

public class LoadLineReader {

	public long timeInSec = -1;
	public long ratePerSec = -1;
	public double[] transactionRatios = null;

	public LoadLineReader(String fileLine) {

		assert (fileLine != null);
		StringTokenizer st = new StringTokenizer(fileLine);

		transactionRatios = new double[st.countTokens() - 2];

		timeInSec = Long.parseLong(st.nextToken());
		ratePerSec = Long.parseLong(st.nextToken());

		for (int i = 0; i < transactionRatios.length; i++)
			transactionRatios[i] = Double.parseDouble(st.nextToken());

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
