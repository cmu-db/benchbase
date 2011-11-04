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
package com.oltpbenchmark.wikipedia;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import com.oltpbenchmark.TransactionTypes;

import ch.ethz.ssh2.util.Tokenizer;

public class TransactionSelector {

	String filename;
	DataInputStream dis = null;
	Random r = null;
	TransactionTypes transTypes;
	static final double READ_WRITE_RATIO = 11.8; // from
													// http://www.globule.org/publi/WWADH_comnet2009.html

	public TransactionSelector(String filename, TransactionTypes transTypes) throws FileNotFoundException {
		this.transTypes = transTypes;
		r = new Random();
		this.filename = filename;

		if(filename==null || filename.isEmpty())
			throw new FileNotFoundException("You must specify a filename to instantiate the TransactionSelector... (probably missing in your workload configuration?)");
		
		File file = new File(filename);
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		fis = new FileInputStream(file);

		// Here BufferedInputStream is added for fast reading.
		bis = new BufferedInputStream(fis);
		dis = new DataInputStream(bis);
		dis.mark(1024 * 1024 * 1024);

	}

	public synchronized WikipediaOperation nextTransaction() throws IOException {
		if (dis.available() == 0)
			dis.reset();

		return readNextTransaction();
	}

	private WikipediaOperation readNextTransaction() throws IOException {
		String line = dis.readLine();
		String[] sa = Tokenizer.parseTokens(line, ' ');

		int user = Integer.parseInt(sa[0]);

		return new WikipediaOperation(user, Integer.parseInt(sa[1]), sa[2]);
	}

	public ArrayList<WikipediaOperation> readAll() throws IOException {
		ArrayList<WikipediaOperation> transactions = new ArrayList<WikipediaOperation>();

		while (dis.available() > 0) {
			transactions.add(readNextTransaction());
		}

		return transactions;
	}

	public void close() throws IOException {
		dis.close();
	}

}
