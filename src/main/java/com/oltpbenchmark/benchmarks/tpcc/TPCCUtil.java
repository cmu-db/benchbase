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


package com.oltpbenchmark.benchmarks.tpcc;

import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import static com.oltpbenchmark.benchmarks.tpcc.TPCCConfig.*;

public class TPCCUtil {

    /**
     * Creates a Customer object from the current row in the given ResultSet.
     * The caller is responsible for closing the ResultSet.
     *
     * @param rs an open ResultSet positioned to the desired row
     * @return the newly created Customer object
     * @throws SQLException for problems getting data from row
     */
    public static Customer newCustomerFromResults(ResultSet rs)
            throws SQLException {
        Customer c = new Customer();
        // TODO: Use column indices: probably faster?
        c.c_first = rs.getString("c_first");
        c.c_middle = rs.getString("c_middle");
        c.c_street_1 = rs.getString("c_street_1");
        c.c_street_2 = rs.getString("c_street_2");
        c.c_city = rs.getString("c_city");
        c.c_state = rs.getString("c_state");
        c.c_zip = rs.getString("c_zip");
        c.c_phone = rs.getString("c_phone");
        c.c_credit = rs.getString("c_credit");
        c.c_credit_lim = rs.getFloat("c_credit_lim");
        c.c_discount = rs.getFloat("c_discount");
        c.c_balance = rs.getFloat("c_balance");
        c.c_ytd_payment = rs.getFloat("c_ytd_payment");
        c.c_payment_cnt = rs.getInt("c_payment_cnt");
        c.c_since = rs.getTimestamp("c_since");
        return c;
    }

    private static final RandomGenerator ran = new RandomGenerator(0);

    public static String randomStr(int strLen) {
        if (strLen > 1) {
            return ran.astring(strLen - 1, strLen - 1);
        } else {
            return "";
        }
    }

    public static String randomNStr(int stringLength) {
        if (stringLength > 0) {
            return ran.nstring(stringLength, stringLength);
        } else {
            return "";
        }
    }

    public static String getCurrentTime() {
        return dateFormat.format(new java.util.Date());
    }

    public static String formattedDouble(double d) {
        String dS = "" + d;
        return dS.length() > 6 ? dS.substring(0, 6) : dS;
    }

    // TODO: TPCC-C 2.1.6: For non-uniform random number generation, the
    // constants for item id,
    // customer id and customer name are supposed to be selected ONCE and reused

    // We just hardcode one selection of parameters here, but we should generate
    // these each time.
    private static final int OL_I_ID_C = 7911; // in range [0, 8191]
    private static final int C_ID_C = 259; // in range [0, 1023]
    // NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must
    // be within [65, 119]
    private static final int C_LAST_LOAD_C = 157; // in range [0, 255]
    private static final int C_LAST_RUN_C = 223; // in range [0, 255]

    public static int getItemID(Random r) {
        return nonUniformRandom(8191, OL_I_ID_C, 1, configItemCount, r);
    }

    public static int getCustomerID(Random r) {
        return nonUniformRandom(1023, C_ID_C, 1, configCustPerDist, r);
    }

    public static String getLastName(int num) {
        return nameTokens[num / 100] + nameTokens[(num / 10) % 10]
                + nameTokens[num % 10];
    }

    public static String getNonUniformRandomLastNameForRun(Random r) {
        return getLastName(nonUniformRandom(255, C_LAST_RUN_C, 0, 999, r));
    }

    public static String getNonUniformRandomLastNameForLoad(Random r) {
        return getLastName(nonUniformRandom(255, C_LAST_LOAD_C, 0, 999, r));
    }

    public static int randomNumber(int min, int max, Random r) {
        return (int) (r.nextDouble() * (max - min + 1) + min);
    }

    public static int nonUniformRandom(int A, int C, int min, int max, Random r) {
        return (((randomNumber(0, A, r) | randomNumber(min, max, r)) + C) % (max
                - min + 1))
                + min;
    }

}
