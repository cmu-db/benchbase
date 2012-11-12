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
