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
package com.oltpbenchmark.benchmarks.tpcc.pojo;

import java.io.Serializable;

public class Warehouse implements Serializable {

	public int w_id; // PRIMARY KEY
	public float w_ytd;
	public float w_tax;
	public String w_name;
	public String w_street_1;
	public String w_street_2;
	public String w_city;
	public String w_state;
	public String w_zip;

	@Override
	public String toString() {
		return ("\n***************** Warehouse ********************"
				+ "\n*       w_id = " + w_id + "\n*      w_ytd = " + w_ytd
				+ "\n*      w_tax = " + w_tax + "\n*     w_name = " + w_name
				+ "\n* w_street_1 = " + w_street_1 + "\n* w_street_2 = "
				+ w_street_2 + "\n*     w_city = " + w_city
				+ "\n*    w_state = " + w_state + "\n*      w_zip = " + w_zip + "\n**********************************************");
	}

} // end Warehouse
