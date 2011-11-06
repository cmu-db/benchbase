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

public class Customer {

	public int c_id;
	public int c_d_id;
	public int c_w_id;
	public int c_payment_cnt;
	public int c_delivery_cnt;
	public Timestamp c_since;
	public float c_discount;
	public float c_credit_lim;
	public float c_balance;
	public float c_ytd_payment;
	public String c_credit;
	public String c_last;
	public String c_first;
	public String c_street_1;
	public String c_street_2;
	public String c_city;
	public String c_state;
	public String c_zip;
	public String c_phone;
	public String c_middle;
	public String c_data;

	@Override
	public String toString() {
		return ("\n***************** Customer ********************"
				+ "\n*           c_id = "
				+ c_id
				+ "\n*         c_d_id = "
				+ c_d_id
				+ "\n*         c_w_id = "
				+ c_w_id
				+ "\n*     c_discount = "
				+ c_discount
				+ "\n*       c_credit = "
				+ c_credit
				+ "\n*         c_last = "
				+ c_last
				+ "\n*        c_first = "
				+ c_first
				+ "\n*   c_credit_lim = "
				+ c_credit_lim
				+ "\n*      c_balance = "
				+ c_balance
				+ "\n*  c_ytd_payment = "
				+ c_ytd_payment
				+ "\n*  c_payment_cnt = "
				+ c_payment_cnt
				+ "\n* c_delivery_cnt = "
				+ c_delivery_cnt
				+ "\n*     c_street_1 = "
				+ c_street_1
				+ "\n*     c_street_2 = "
				+ c_street_2
				+ "\n*         c_city = "
				+ c_city
				+ "\n*        c_state = "
				+ c_state
				+ "\n*          c_zip = "
				+ c_zip
				+ "\n*        c_phone = "
				+ c_phone
				+ "\n*        c_since = "
				+ c_since
				+ "\n*       c_middle = "
				+ c_middle
				+ "\n*         c_data = " + c_data + "\n**********************************************");
	}

} // end Customer
