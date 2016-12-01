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


package com.oltpbenchmark.benchmarks.tpcc.jdbc;

/*
 * jdbcIO - execute JDBC statements
 *
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oltpbenchmark.benchmarks.tpcc.pojo.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Oorder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.OrderLine;


public class HSQLDBjdbcIO {

	public void insertOrder(PreparedStatement ordrPrepStmt, Oorder oorder) {

		try {

			ordrPrepStmt.setInt(1, oorder.o_id);
			ordrPrepStmt.setInt(2, oorder.o_w_id);
			ordrPrepStmt.setInt(3, oorder.o_d_id);
			ordrPrepStmt.setInt(4, oorder.o_c_id);
			ordrPrepStmt.setInt(5, oorder.o_carrier_id);
			ordrPrepStmt.setInt(6, oorder.o_ol_cnt);
			ordrPrepStmt.setInt(7, oorder.o_all_local);
			ordrPrepStmt.setTimestamp(8, oorder.o_entry_d);
			ordrPrepStmt.execute();

		} catch (SQLException se) {
			System.out.println(se.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}

	} // end insertOrder()

	public void insertNewOrder(PreparedStatement nworPrepStmt,
			NewOrder new_order) {

		try {
			nworPrepStmt.setInt(1, new_order.no_w_id);
			nworPrepStmt.setInt(2, new_order.no_d_id);
			nworPrepStmt.setInt(3, new_order.no_o_id);

			nworPrepStmt.execute();

		} catch (SQLException se) {
			System.out.println(se.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}

	} // end insertNewOrder()

	public void insertOrderLine(PreparedStatement orlnPrepStmt,
			OrderLine order_line) {

		try {
			orlnPrepStmt.setInt(1, order_line.ol_w_id);
			orlnPrepStmt.setInt(2, order_line.ol_d_id);
			orlnPrepStmt.setInt(3, order_line.ol_o_id);
			orlnPrepStmt.setInt(4, order_line.ol_number);
			orlnPrepStmt.setLong(5, order_line.ol_i_id);
			orlnPrepStmt.setTimestamp(6, order_line.ol_delivery_d);

			orlnPrepStmt.setDouble(7, order_line.ol_amount);
			orlnPrepStmt.setLong(8, order_line.ol_supply_w_id);
			orlnPrepStmt.setDouble(9, order_line.ol_quantity);
			orlnPrepStmt.setString(10, order_line.ol_dist_info);

			orlnPrepStmt.execute();

		} catch (SQLException se) {
			System.out.println(se.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}

	} // end insertOrderLine()

} // end class jdbcIO()
