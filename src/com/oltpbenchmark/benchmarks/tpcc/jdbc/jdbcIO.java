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
import java.sql.Timestamp;
import java.sql.Types;

import com.oltpbenchmark.benchmarks.tpcc.pojo.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Oorder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.OrderLine;


public class jdbcIO {

	public void insertOrder(PreparedStatement ordrPrepStmt, Oorder oorder) {

		try {

			ordrPrepStmt.setInt(1, oorder.o_w_id);
			ordrPrepStmt.setInt(2, oorder.o_d_id);
			ordrPrepStmt.setInt(3, oorder.o_id);
			ordrPrepStmt.setInt(4, oorder.o_c_id);
			if (oorder.o_carrier_id != null) {
				ordrPrepStmt.setInt(5, oorder.o_carrier_id);
			} else {
				ordrPrepStmt.setNull(5, Types.INTEGER);
			}
			ordrPrepStmt.setInt(6, oorder.o_ol_cnt);
			ordrPrepStmt.setInt(7, oorder.o_all_local);
			Timestamp entry_d = new java.sql.Timestamp(oorder.o_entry_d);
			ordrPrepStmt.setTimestamp(8, entry_d);

			ordrPrepStmt.addBatch();
			//ordrPrepStmt.execute();
			
		} catch (SQLException se) {
			throw new RuntimeException(se);
		}

	} // end insertOrder()

	public void insertNewOrder(PreparedStatement nworPrepStmt,
			NewOrder new_order) {

		try {
			nworPrepStmt.setInt(1, new_order.no_w_id);
			nworPrepStmt.setInt(2, new_order.no_d_id);
			nworPrepStmt.setInt(3, new_order.no_o_id);

			nworPrepStmt.addBatch();

		} catch (SQLException se) {
			throw new RuntimeException(se);
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

			Timestamp delivery_d = null;
			if (order_line.ol_delivery_d != null)
				delivery_d = new Timestamp(order_line.ol_delivery_d);
            if (delivery_d == null)
                orlnPrepStmt.setNull(6, 0);
            else
			orlnPrepStmt.setTimestamp(6, delivery_d);

			orlnPrepStmt.setDouble(7, order_line.ol_amount);
			orlnPrepStmt.setLong(8, order_line.ol_supply_w_id);
			orlnPrepStmt.setDouble(9, order_line.ol_quantity);
			orlnPrepStmt.setString(10, order_line.ol_dist_info);

			orlnPrepStmt.addBatch();

		} catch (SQLException se) {
			throw new RuntimeException(se);
		}

	} // end insertOrderLine()

} // end class jdbcIO()
