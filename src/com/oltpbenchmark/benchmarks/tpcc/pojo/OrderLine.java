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

public class OrderLine {

	public int ol_w_id;
	public int ol_d_id;
	public int ol_o_id;
	public int ol_number;
	public int ol_i_id;
	public int ol_supply_w_id;
	public int ol_quantity;
	public Long ol_delivery_d;
	public float ol_amount;
	public String ol_dist_info;

	@Override
	public String toString() {
		return ("\n***************** OrderLine ********************"
				+ "\n*        ol_w_id = " + ol_w_id + "\n*        ol_d_id = "
				+ ol_d_id + "\n*        ol_o_id = " + ol_o_id
				+ "\n*      ol_number = " + ol_number + "\n*        ol_i_id = "
				+ ol_i_id + "\n*  ol_delivery_d = " + ol_delivery_d
				+ "\n*      ol_amount = " + ol_amount + "\n* ol_supply_w_id = "
				+ ol_supply_w_id + "\n*    ol_quantity = " + ol_quantity
				+ "\n*   ol_dist_info = " + ol_dist_info + "\n**********************************************");
	}

} // end OrderLine
