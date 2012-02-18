/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
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