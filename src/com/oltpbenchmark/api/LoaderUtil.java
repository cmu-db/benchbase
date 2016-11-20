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


package com.oltpbenchmark.api;

import java.util.Random;

public class LoaderUtil {

	private final static String[] nameTokens = {
		"BAR", "OUGHT", "ABLE", "PRI",
		"PRES", "ESE", "ANTI", "CALLY",
		"ATION", "EING"
	};
	
	/**
	 * This is slow! Use TextGenerator.randomStr()
	 * @param strLen
	 * @return
	 */
	@Deprecated
	public static String randomStr(long strLen) {

		char freshChar;		
		StringBuilder sb = new StringBuilder();
		
		while (sb.length() < (strLen - 1)) {
			freshChar = (char) (Math.random() * 128);
			if (Character.isLetter(freshChar)) {
				sb.append(freshChar);
			}
		}

		return sb.toString();

	} // end randomStr

	public static String blockBuilder(String piece, int repeat){
	    StringBuilder sb = new StringBuilder("<HTML>");
	    for (int i=0;i<repeat;i++) sb.append(piece);
	    sb.append("</HTML>");
	    return sb.toString();
	}
	
	public static String randomNStr(Random r, int stringLength) {
		StringBuilder output = new StringBuilder();
		char base = '0';
		while (output.length() < stringLength) {
			char next = (char) (base + r.nextInt(10));
			output.append(next);
		}
		return output.toString();
	}

	public static String formattedDouble(double d) {
		String dS = "" + d;
		return dS.length() > 6 ? dS.substring(0, 6) : dS;
	}
	
	public static String getLastName(int num) {
		return nameTokens[num / 100] + nameTokens[(num / 10) % 10]
				+ nameTokens[num % 10];
	}

	public static String getNonUniformRandomLastName(Random r) {
		return getLastName(nonUniformRandom(255, 157, 0, 999, r));
	}

	public static int nonUniformRandom(int A, int C, int min, int max, Random r) {
		return (((randomNumber(0, A, r) | randomNumber(min, max, r)) + C) % (max
				- min + 1))
				+ min;
	}
	public static int randomNumber(int min, int max, Random r) {
		return (int) (r.nextDouble() * (max - min + 1) + min);
	}

} // end jTPCCUtil
