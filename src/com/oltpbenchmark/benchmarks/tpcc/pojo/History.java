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

import java.sql.Timestamp;

public class History {

	public int h_c_id;
	public int h_c_d_id;
	public int h_c_w_id;
	public int h_d_id;
	public int h_w_id;
	public Timestamp h_date;
	public float h_amount;
	public String h_data;

	@Override
	public String toString() {
		return ("\n***************** History ********************"
				+ "\n*   h_c_id = " + h_c_id + "\n* h_c_d_id = " + h_c_d_id
				+ "\n* h_c_w_id = " + h_c_w_id + "\n*   h_d_id = " + h_d_id
				+ "\n*   h_w_id = " + h_w_id + "\n*   h_date = " + h_date
				+ "\n* h_amount = " + h_amount + "\n*   h_data = " + h_data + "\n**********************************************");
	}

} // end History
