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

package com.oltpbenchmark.benchmarks.seats.procedures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Procedure;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;

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
    
    public ResultSet[] run(Connection conn) throws SQLException {
        ResultSet results[] = new ResultSet[6];
        int result_idx = 0;
        
        results[result_idx++] = this.getPreparedStatement(conn, getConfigProfile).executeQuery();
        results[result_idx++] = this.getPreparedStatement(conn, getConfigHistogram).executeQuery();
        results[result_idx++] = this.getPreparedStatement(conn, getCountryCodes).executeQuery();
        results[result_idx++] = this.getPreparedStatement(conn, getAirportCodes).executeQuery();
        results[result_idx++] = this.getPreparedStatement(conn, getAirlineCodes).executeQuery();
        results[result_idx++] = this.getPreparedStatement(conn, getFlights).executeQuery();
        return (results);
    }
}
