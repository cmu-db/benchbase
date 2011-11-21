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
package com.oltpbenchmark.benchmarks.tpcc;

/*
 * jTPCCConfig - Basic configuration parameters for jTPCC
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.text.SimpleDateFormat;

public final class jTPCCConfig {

	// TODO: This was final; Modified by TPCCRateLimited. Better system?
	public static boolean TERMINAL_MESSAGES = true;

	public static enum TransactionType {
		INVALID, // Exists so the order is the same as the constants below
		NEW_ORDER, PAYMENT, ORDER_STATUS, DELIVERY, STOCK_LEVEL
	}

	// TODO: Remove these constants
	public final static int NEW_ORDER = 1, PAYMENT = 2, ORDER_STATUS = 3,
			DELIVERY = 4, STOCK_LEVEL = 5;

	public final static String[] nameTokens = { "BAR", "OUGHT", "ABLE", "PRI",
			"PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };

	public final static String terminalPrefix = "Term-";
	public final static String reportFilePrefix = "reports/BenchmarkSQL_session_";

	public final static SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public final static int configCommitCount = 1000; // commit every n records
	public final static int configWhseCount = 1;
	public final static int configItemCount = 100000; // tpc-c std = 100,000
	public final static int configDistPerWhse = 10; // tpc-c std = 10
	public final static int configCustPerDist = 3000; // tpc-c std = 3,000

	/** An invalid item id used to rollback a new order transaction. */
	public static final int INVALID_ITEM_ID = -12345;
}
