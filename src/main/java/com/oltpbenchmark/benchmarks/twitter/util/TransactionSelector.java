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


package com.oltpbenchmark.benchmarks.twitter.util;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import com.oltpbenchmark.api.TransactionTypes;

import ch.ethz.ssh2.util.Tokenizer;

public class TransactionSelector {

	String filename,filename2;
	DataInputStream dis,dis2 = null;
	Random r = null;
	TransactionTypes transTypes;
	static final double READ_WRITE_RATIO = 11.8; // from
													// http://www.globule.org/publi/WWADH_comnet2009.html

	public TransactionSelector(String filename, String filename2, TransactionTypes transTypes) throws FileNotFoundException {
		this.transTypes = transTypes;
		r = new Random();
		this.filename = filename;
		this.filename2 = filename2;

		if(filename==null || filename.isEmpty())
			throw new FileNotFoundException("You must specify a filename to instantiate the TransactionSelector... (probably missing in your workload configuration?)");

		if(filename2==null || filename2.isEmpty())
			throw new FileNotFoundException("You must specify a filename to instantiate the TransactionSelector... (probably missing in your workload configuration?)");

		
		File file = new File(filename);
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		fis = new FileInputStream(file);

		// Here BufferedInputStream is added for fast reading.
		bis = new BufferedInputStream(fis);
		dis = new DataInputStream(bis);
		dis.mark(1024 * 1024 * 1024);

		File file2 = new File(filename2);
		FileInputStream fis2 = null;
		BufferedInputStream bis2 = null;
		fis2 = new FileInputStream(file2);

		// Here BufferedInputStream is added for fast reading.
		bis2 = new BufferedInputStream(fis2);
		dis2 = new DataInputStream(bis2);
		dis2.mark(1024 * 1024 * 1024);
	
	}

	public synchronized TwitterOperation nextTransaction() throws IOException {
		if (dis.available() == 0)
			dis.reset();
		if (dis2.available() == 0)
			dis2.reset();

		return readNextTransaction();
	}

	private TwitterOperation readNextTransaction() throws IOException {
		String line = dis.readLine();
		String[] sa = Tokenizer.parseTokens(line, ' ');
		int tweetid = Integer.parseInt(sa[0]);

		String line2 = dis2.readLine();
		String[] sa2 = Tokenizer.parseTokens(line2, ' ');
		int uid = Integer.parseInt(sa2[0]);
		
		return new TwitterOperation(tweetid,uid);
	}

	public ArrayList<TwitterOperation> readAll() throws IOException {
		ArrayList<TwitterOperation> transactions = new ArrayList<TwitterOperation>();

		while (dis.available() > 0 && dis2.available() > 0) {
			transactions.add(readNextTransaction());
		}

		return transactions;
	}

	public void close() throws IOException {
		dis.close();
		dis2.close();
	}

}
