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

public class District implements Serializable {

	public int d_id;
	public int d_w_id;
	public int d_next_o_id;
	public float d_ytd;
	public float d_tax;
	public String d_name;
	public String d_street_1;
	public String d_street_2;
	public String d_city;
	public String d_state;
	public String d_zip;

	@Override
	public String toString() {
		return ("\n***************** District ********************"
				+ "\n*        d_id = "
				+ d_id
				+ "\n*      d_w_id = "
				+ d_w_id
				+ "\n*       d_ytd = "
				+ d_ytd
				+ "\n*       d_tax = "
				+ d_tax
				+ "\n* d_next_o_id = "
				+ d_next_o_id
				+ "\n*      d_name = "
				+ d_name
				+ "\n*  d_street_1 = "
				+ d_street_1
				+ "\n*  d_street_2 = "
				+ d_street_2
				+ "\n*      d_city = "
				+ d_city
				+ "\n*     d_state = " + d_state + "\n*       d_zip = " + d_zip + "\n**********************************************");
	}

} // end District
