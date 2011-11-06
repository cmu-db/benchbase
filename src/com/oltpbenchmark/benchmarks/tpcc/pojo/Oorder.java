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

public class Oorder {

	public int o_id;
	public int o_w_id;
	public int o_d_id;
	public int o_c_id;
	public Integer o_carrier_id;
	public int o_ol_cnt;
	public int o_all_local;
	public long o_entry_d;

	@Override
	public String toString() {
		java.sql.Timestamp entry_d = new java.sql.Timestamp(o_entry_d);

		return ("\n***************** Oorder ********************"
				+ "\n*         o_id = " + o_id + "\n*       o_w_id = " + o_w_id
				+ "\n*       o_d_id = " + o_d_id + "\n*       o_c_id = "
				+ o_c_id + "\n* o_carrier_id = " + o_carrier_id
				+ "\n*     o_ol_cnt = " + o_ol_cnt + "\n*  o_all_local = "
				+ o_all_local + "\n*    o_entry_d = " + entry_d + "\n**********************************************");
	}

} // end Oorder
