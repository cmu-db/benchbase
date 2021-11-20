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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class FindFlights extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(FindFlights.class);


    public final SQLStmt GetNearbyAirports = new SQLStmt(
            "SELECT * " +
                    "  FROM " + SEATSConstants.TABLENAME_AIRPORT_DISTANCE +
                    " WHERE D_AP_ID0 = ? " +
                    "   AND D_DISTANCE <= ? " +
                    " ORDER BY D_DISTANCE ASC "
    );

    public final SQLStmt GetAirportInfo = new SQLStmt(
            "SELECT AP_CODE, AP_NAME, AP_CITY, AP_LONGITUDE, AP_LATITUDE, " +
                    " CO_ID, CO_NAME, CO_CODE_2, CO_CODE_3 " +
                    " FROM " + SEATSConstants.TABLENAME_AIRPORT + ", " +
                    SEATSConstants.TABLENAME_COUNTRY +
                    " WHERE AP_ID = ? AND AP_CO_ID = CO_ID "
    );

    private final static String BaseGetFlights =
            "SELECT F_ID, F_AL_ID, F_SEATS_LEFT, " +
                    " F_DEPART_AP_ID, F_DEPART_TIME, F_ARRIVE_AP_ID, F_ARRIVE_TIME, " +
                    " AL_NAME, AL_IATTR00, AL_IATTR01 " +
                    " FROM " + SEATSConstants.TABLENAME_FLIGHT + ", " +
                    SEATSConstants.TABLENAME_AIRLINE +
                    " WHERE F_DEPART_AP_ID = ? " +
                    "   AND F_DEPART_TIME >= ? AND F_DEPART_TIME <= ? " +
                    "   AND F_AL_ID = AL_ID " +
                    "   AND F_ARRIVE_AP_ID IN (??)";

    public final SQLStmt GetFlights1 = new SQLStmt(BaseGetFlights, 1);
    public final SQLStmt GetFlights2 = new SQLStmt(BaseGetFlights, 2);
    public final SQLStmt GetFlights3 = new SQLStmt(BaseGetFlights, 3);

    public List<Object[]> run(Connection conn, long depart_aid, long arrive_aid, Timestamp start_date, Timestamp end_date, long distance) throws SQLException {
        try {


            final List<Long> arrive_aids = new ArrayList<>();
            arrive_aids.add(arrive_aid);

            final List<Object[]> finalResults = new ArrayList<>();

            if (distance > 0) {
                // First get the nearby airports for the departure and arrival cities
                try (PreparedStatement nearby_stmt = this.getPreparedStatement(conn, GetNearbyAirports, depart_aid, distance)) {
                    try (ResultSet nearby_results = nearby_stmt.executeQuery()) {
                        while (nearby_results.next()) {
                            long aid = nearby_results.getLong(1);
                            double aid_distance = nearby_results.getDouble(2);

                            LOG.debug("DEPART NEARBY: {} distance={} miles", aid, aid_distance);

                            arrive_aids.add(aid);
                        }
                    }
                }
            }

            // H-Store doesn't support IN clauses, so we'll only get nearby flights to nearby arrival cities
            int num_nearby = arrive_aids.size();
            if (num_nearby > 0) {
                SQLStmt sqlStmt;
                if (num_nearby == 1) {
                    sqlStmt = GetFlights1;
                } else if (num_nearby == 2) {
                    sqlStmt = GetFlights2;
                } else {
                    sqlStmt = GetFlights3;
                }


                try (PreparedStatement f_stmt = this.getPreparedStatement(conn, sqlStmt)) {

                    // Set Parameters
                    f_stmt.setLong(1, depart_aid);
                    f_stmt.setTimestamp(2, start_date);
                    f_stmt.setTimestamp(3, end_date);
                    for (int i = 0, cnt = Math.min(3, num_nearby); i < cnt; i++) {
                        f_stmt.setLong(4 + i, arrive_aids.get(i));
                    }


                    // Process Result
                    try (ResultSet flightResults = f_stmt.executeQuery()) {


                        try (PreparedStatement ai_stmt = this.getPreparedStatement(conn, GetAirportInfo)) {
                            while (flightResults.next()) {
                                long f_depart_airport = flightResults.getLong(4);
                                long f_arrive_airport = flightResults.getLong(6);

                                Object[] row = new Object[13];
                                int r = 0;

                                row[r++] = flightResults.getString(1);    // [00] F_ID
                                row[r++] = flightResults.getLong(3);    // [01] SEATS_LEFT
                                row[r++] = flightResults.getString(8);  // [02] AL_NAME

                                // DEPARTURE AIRPORT
                                ai_stmt.setLong(1, f_depart_airport);
                                try (ResultSet ai_results = ai_stmt.executeQuery()) {
                                    ai_results.next();

                                    row[r++] = flightResults.getDate(5);    // [03] DEPART_TIME
                                    row[r++] = ai_results.getString(1);     // [04] DEPART_AP_CODE
                                    row[r++] = ai_results.getString(2);     // [05] DEPART_AP_NAME
                                    row[r++] = ai_results.getString(3);     // [06] DEPART_AP_CITY
                                    row[r++] = ai_results.getString(7);     // [07] DEPART_AP_COUNTRY
                                }

                                // ARRIVAL AIRPORT
                                ai_stmt.setLong(1, f_arrive_airport);
                                try (ResultSet ai_results = ai_stmt.executeQuery()) {
                                    ai_results.next();

                                    row[r++] = flightResults.getDate(7);    // [08] ARRIVE_TIME
                                    row[r++] = ai_results.getString(1);     // [09] ARRIVE_AP_CODE
                                    row[r++] = ai_results.getString(2);     // [10] ARRIVE_AP_NAME
                                    row[r++] = ai_results.getString(3);     // [11] ARRIVE_AP_CITY
                                    row[r] = ai_results.getString(7);     // [12] ARRIVE_AP_COUNTRY
                                }

                                finalResults.add(row);

                            }
                        }
                    }
                }

            }

            LOG.debug("Flight Information:\n{}", finalResults);

            return (finalResults);
        } catch (SQLException esql) {
            LOG.error("caught SQLException in FindFlights:{}", esql, esql);
            throw esql;
        } catch (Exception e) {
            LOG.error("caught Exception in FindFlights:{}", e, e);
        }
        return null;
    }
}
