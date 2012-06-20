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
package com.oltpbenchmark.benchmarks.chbenchmark.pojo;

//>>> CH-benCHmark
public class Supplier {

	public int su_suppkey; // PRIMARY KEY
	public String su_name;
	public String su_address;
	public int su_nationkey;
	public String su_phone;
	public float su_acctbal;
	public String su_comment;

	@Override
	public String toString() {
		return ("\n***************** Supplier ********************"
				+ "\n*    su_suppkey = " + su_suppkey + "\n*  su_name = " + su_name
				+ "\n*    su_address = " + su_address + "\n*  su_nationkey = " + su_nationkey
				+ "\n*    su_phone = " + su_phone + "\n*  su_acctbal = " + su_acctbal
				+ "\n* su_comment = " + su_comment + "\n**********************************************");
	}

} // end Supplier

//<<< CH-benCHmark