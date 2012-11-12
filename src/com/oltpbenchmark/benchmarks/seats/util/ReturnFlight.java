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

import java.sql.Timestamp;
import java.util.Calendar;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;

public class ReturnFlight implements Comparable<ReturnFlight> {
    
    private final CustomerId customer_id;
    private final long return_airport_id;
    private final Timestamp return_date;
    
    public ReturnFlight(CustomerId customer_id, long return_airport_id, Timestamp flight_date, int return_days) {
        this.customer_id = customer_id;
        this.return_airport_id = return_airport_id;
        this.return_date = ReturnFlight.calculateReturnDate(flight_date, return_days);
    }
    
    /**
     * 
     * @param flight_date
     * @param return_days
     * @return
     */
    protected static final Timestamp calculateReturnDate(Timestamp flight_date, int return_days) {
        assert(return_days >= 0);
        // Round this to the start of the day
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(flight_date.getTime() + (return_days * SEATSConstants.MILLISECONDS_PER_DAY));
        
        int year = cal.get(Calendar.YEAR);
        int month= cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DAY_OF_MONTH);
        
        cal.clear();
        cal.set(year, month, day);
        return (new Timestamp(cal.getTime().getTime()));
    }
    
    /**
     * @return the customer_id
     */
    public CustomerId getCustomerId() {
        return customer_id;
    }

    /**
     * @return the return_airport_id
     */
    public long getReturnAirportId() {
        return return_airport_id;
    }

    /**
     * @return the return_time
     */
    public Timestamp getReturnDate() {
        return return_date;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        
        if (!(obj instanceof ReturnFlight) || obj == null)
            return false;
        
        ReturnFlight o = (ReturnFlight)obj;
        return (this.customer_id.equals(o.customer_id) &&
                 this.return_airport_id == o.return_airport_id &&
                 this.return_date.equals(o.return_date));
    }

    @Override
    public int compareTo(ReturnFlight o) {
        if (this.customer_id.equals(o.customer_id) &&
            this.return_airport_id == o.return_airport_id &&
            this.return_date.equals(o.return_date)) {
            return (0);
        }
        // Otherwise order by time
        return (this.return_date.compareTo(o.return_date));
    }
    
    @Override
    public String toString() {
        return String.format("ReturnFlight{%s,airport=%s,date=%s}",
                             this.customer_id, this.return_airport_id, this.return_date);
    }

}
