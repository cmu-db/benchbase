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

import junit.framework.TestCase;

public class TestCustomerId extends TestCase {

    private final long base_ids[]    = { 66666, 77777, 88888 };  
    private final long airport_ids[] = { 123, 1234, 12345 };
    
    /**
     * testCustomerId
     */
    public void testCustomerId() {
        for (long base_id : this.base_ids) {
            for (long airport_id : this.airport_ids) {
                CustomerId customer_id = new CustomerId((int)base_id, airport_id);
                assertNotNull(customer_id);
                assertEquals(base_id, customer_id.getId());
                assertEquals(airport_id, customer_id.getDepartAirportId());
            } // FOR
        } // FOR
    }
    
    /**
     * testCustomerIdEncode
     */
    public void testCustomerIdEncode() {
        for (long base_id : this.base_ids) {
            for (long airport_id : this.airport_ids) {
                long encoded = new CustomerId((int)base_id, airport_id).encode();
//                System.err.println("base_id=" + base_id);
//                System.err.println("airport_id=" + airport_id);
//                System.err.println("encodd=" + encoded);
//                System.exit(1);
                assert(encoded >= 0);
                
                CustomerId customer_id = new CustomerId(encoded);
                assertNotNull(customer_id);
                assertEquals(base_id, customer_id.getId());
                assertEquals(airport_id, customer_id.getDepartAirportId());
            } // FOR
        } // FOR
    }
    
    /**
     * testCustomerIdDecode
     */
    public void testCustomerIdDecode() {
        for (long base_id : this.base_ids) {
            for (long airport_id : this.airport_ids) {
                long values[] = { base_id, airport_id };
                long encoded = new CustomerId((int)base_id, airport_id).encode();
                assert(encoded >= 0);

                long new_values[] = new CustomerId(encoded).toArray();
                assertEquals(values.length, new_values.length);
                for (int i = 0; i < new_values.length; i++) {
                    assertEquals(values[i], new_values[i]);
                } // FOR
            } // FOR
        } // FOR
    }
}