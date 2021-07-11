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

package com.oltpbenchmark.benchmarks.seats.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.seats.SEATSConstants;
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
            "SELECT * FROM " + SEATSConstants.TABLENAME_CONFIG_PROFILE
    );

    public final SQLStmt getConfigHistogram = new SQLStmt(
            "SELECT * FROM " + SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS
    );

    public final SQLStmt getCountryCodes = new SQLStmt(
            "SELECT CO_ID, CO_CODE_3 FROM " + SEATSConstants.TABLENAME_COUNTRY
    );

    public final SQLStmt getAirportCodes = new SQLStmt(
            "SELECT AP_ID, AP_CODE FROM " + SEATSConstants.TABLENAME_AIRPORT
    );

    public final SQLStmt getAirlineCodes = new SQLStmt(
            "SELECT AL_ID, AL_IATA_CODE FROM " + SEATSConstants.TABLENAME_AIRLINE +
                    " WHERE AL_IATA_CODE != ''"
    );

    public final SQLStmt getFlights = new SQLStmt(
            "SELECT f_id FROM " + SEATSConstants.TABLENAME_FLIGHT +
                    " ORDER BY F_DEPART_TIME DESC " +
                    " LIMIT " + SEATSConstants.CACHE_LIMIT_FLIGHT_IDS
    );

    public Config run(Connection conn) throws SQLException {

        List<Object[]> configProfile;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getConfigProfile)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                configProfile = SQLUtil.toList(resultSet);
            }
        }

        List<Object[]> histogram;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getConfigHistogram)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                histogram = SQLUtil.toList(resultSet);
            }
        }

        List<Object[]> countryCodes;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getCountryCodes)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                countryCodes = SQLUtil.toList(resultSet);
            }
        }

        List<Object[]> airportCodes;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getAirportCodes)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                airportCodes = SQLUtil.toList(resultSet);
            }
        }

        List<Object[]> airlineCodes;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getAirlineCodes)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                airlineCodes = SQLUtil.toList(resultSet);
            }
        }

        List<Object[]> flights;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getFlights)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                flights = SQLUtil.toList(resultSet);
            }
        }

        return new Config(configProfile, histogram, countryCodes, airportCodes, airlineCodes, flights);
    }
}
