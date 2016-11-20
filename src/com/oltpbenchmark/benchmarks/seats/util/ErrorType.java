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


package com.oltpbenchmark.benchmarks.seats.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Internal Error Codes
 * @author pavlo
 */
public enum ErrorType {
    INVALID_FLIGHT_ID,
    INVALID_CUSTOMER_ID,
    NO_MORE_SEATS,
    SEAT_ALREADY_RESERVED,
    CUSTOMER_ALREADY_HAS_SEAT,
    VALIDITY_ERROR,
    UNKNOWN;
    
    private final String errorCode;
    private final static Pattern p = Pattern.compile("^(USER ABORT:[\\s]+)?E([\\d]{4})");
    
    private ErrorType() {
        this.errorCode = String.format("E%04d", this.ordinal());
    }
    
    public static ErrorType getErrorType(String msg) {
        Matcher m = p.matcher(msg);
        if (m.find()) {
            int idx = Integer.parseInt(m.group(2));
            return ErrorType.values()[idx];
        }
        return (ErrorType.UNKNOWN);
    }
    @Override
    public String toString() {
        return this.errorCode;
    }
}