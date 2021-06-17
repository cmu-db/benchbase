/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import com.oltpbenchmark.util.SQLUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class LoadConfig extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getConfigProfile = new SQLStmt(
            "SELECT * FROM " + AuctionMarkConstants.TABLENAME_CONFIG_PROFILE
    );

    public final SQLStmt getCategoryCounts = new SQLStmt(
            "SELECT i_c_id, COUNT(i_id) FROM " + AuctionMarkConstants.TABLENAME_ITEM +
                    " GROUP BY i_c_id"
    );

    public final SQLStmt getAttributes = new SQLStmt(
            "SELECT gag_id FROM " + AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP
    );

    public final SQLStmt getPendingComments = new SQLStmt(
            "SELECT ic_id, ic_i_id, ic_u_id, ic_buyer_id " +
                    "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT +
                    " WHERE ic_response IS NULL"
    );

    public final SQLStmt getPastItems = new SQLStmt(
            "SELECT i_id, i_current_price, i_end_date, i_num_bids, i_status " +
                    "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " +
                    AuctionMarkConstants.TABLENAME_CONFIG_PROFILE +
                    " WHERE i_status = ? AND i_end_date <= cfp_loader_start " +
                    " ORDER BY i_end_date ASC " +
                    " LIMIT " + AuctionMarkConstants.ITEM_LOADCONFIG_LIMIT
    );

    public final SQLStmt getFutureItems = new SQLStmt(
            "SELECT i_id, i_current_price, i_end_date, i_num_bids, i_status " +
                    "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " +
                    AuctionMarkConstants.TABLENAME_CONFIG_PROFILE +
                    " WHERE i_status = ? AND i_end_date > cfp_loader_start " +
                    " ORDER BY i_end_date ASC " +
                    " LIMIT " + AuctionMarkConstants.ITEM_LOADCONFIG_LIMIT
    );

    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------

    public Config run(Connection conn) throws SQLException {


        List<Object[]> configProfile;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getConfigProfile)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                configProfile = SQLUtil.toList(resultSet);
            }
        }

        List<Object[]> categoryCounts;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getCategoryCounts)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                categoryCounts = SQLUtil.toList(resultSet);
            }
        }

        List<Object[]> attributes;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getAttributes)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                attributes = SQLUtil.toList(resultSet);
            }
        }

        List<Object[]> pendingComments;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getPendingComments)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                pendingComments = SQLUtil.toList(resultSet);
            }
        }

        List<Object[]> openItems;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getFutureItems)) {
            preparedStatement.setLong(1, ItemStatus.OPEN.ordinal());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                openItems = SQLUtil.toList(resultSet);
            }
        }

        List<Object[]> waiting;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getPastItems)) {
            preparedStatement.setLong(1, ItemStatus.WAITING_FOR_PURCHASE.ordinal());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                waiting = SQLUtil.toList(resultSet);
            }
        }

        List<Object[]> closedItems;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getPastItems)) {
            preparedStatement.setLong(1, ItemStatus.CLOSED.ordinal());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                closedItems = SQLUtil.toList(resultSet);
            }
        }

        return new Config(configProfile, categoryCounts, attributes, pendingComments, openItems, waiting, closedItems);
    }
}
