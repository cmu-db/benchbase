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


package com.oltpbenchmark.benchmarks.seats;

import java.util.regex.Pattern;

public abstract class SEATSConstants {

    // ----------------------------------------------------------------
    // STORED PROCEDURE EXECUTION FREQUENCIES (0% - 100%)
    // ----------------------------------------------------------------

    public static final int FREQUENCY_DELETE_RESERVATION = 10;
    public static final int FREQUENCY_FIND_FLIGHTS = 10;
    public static final int FREQUENCY_FIND_OPEN_SEATS = 35;
    public static final int FREQUENCY_NEW_RESERVATION = 20;
    public static final int FREQUENCY_UPDATE_CUSTOMER = 10;
    public static final int FREQUENCY_UPDATE_RESERVATION = 15;

    // ----------------------------------------------------------------
    // FLIGHT CONSTANTS
    // ----------------------------------------------------------------

    /**
     * The different distances that we can look-up for nearby airports
     * This is similar to the customer selecting a dropdown when looking for flights
     */
    // public static final int DISTANCES[] = { 5 }; // , 10, 25, 50, 100 };

    // Zhenwu made the changes. The original code is above
    public static final int[] DISTANCES = {5, 10, 25, 50, 100};

    /**
     * The number of days in the past and future that we will generate flight information for
     */
    public static final int FLIGHTS_DAYS_PAST = 1;
    public static final int FLIGHTS_DAYS_FUTURE = 50;

    /**
     * Average # of flights per day
     * NUM_FLIGHTS_PER_DAY = 15000
     * Source: http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time
     */
    public static final int FLIGHTS_PER_DAY_MIN = 1125;
    public static final int FLIGHTS_PER_DAY_MAX = 1875;

    /**
     * Number of seats available per flight
     * If you change this then you must also change FindOpenSeats
     */
    public static final int FLIGHTS_NUM_SEATS = 150;

    /**
     * How many First Class seats are on a given flight
     * These reservations are more expensive
     */
    public static final int FLIGHTS_FIRST_CLASS_OFFSET = 10;

    /**
     * The rate in which a flight can travel between two airports (miles per hour)
     */
    public static final double FLIGHT_TRAVEL_RATE = 570.0; // Boeing 747

    // ----------------------------------------------------------------
    // CUSTOMER CONSTANTS
    // ----------------------------------------------------------------

    /**
     * Default number of customers in the database
     */
    public static final int CUSTOMERS_COUNT = 100000;

    /**
     * Max Number of FREQUENT_FLYER records per CUSTOMER
     */
    public static final int CUSTOMER_NUM_FREQUENTFLYERS_MIN = 0;
    public static final int CUSTOMER_NUM_FREQUENTFLYERS_MAX = 10;
    public static final double CUSTOMER_NUM_FREQUENTFLYERS_SIGMA = 2.0;

    /**
     * The maximum number of days that we allow a customer to wait before needing
     * a reservation on a return to their original departure airport
     */
    public static final int CUSTOMER_RETURN_FLIGHT_DAYS_MIN = 1;
    public static final int CUSTOMER_RETURN_FLIGHT_DAYS_MAX = 14;

    // ----------------------------------------------------------------
    // RESERVATION CONSTANTS
    // ----------------------------------------------------------------

    public static final int RESERVATION_PRICE_MIN = 100;
    public static final int RESERVATION_PRICE_MAX = 1000;

    public static final int MAX_OPEN_SEATS_PER_TXN = 100;

    // ----------------------------------------------------------------
    // PROBABILITIES
    // ----------------------------------------------------------------

    /**
     * Probability that a customer books a non-roundtrip flight (0% - 100%)
     */
    public static final int PROB_SINGLE_FLIGHT_RESERVATION = 10;

    /**
     * Probability that a customer will invoke DeleteReservation using the string
     * version of their Customer Id (0% - 100%)
     */
    public static final int PROB_DELETE_WITH_CUSTOMER_ID_STR = 20;

    /**
     * Probability that a customer will invoke UpdateCustomer using the string
     * version of their Customer Id (0% - 100%)
     */
    public static final int PROB_UPDATE_WITH_CUSTOMER_ID_STR = 20;

    /**
     * Probability that a customer will invoke DeleteReservation using the string
     * version of their FrequentFlyer Id (0% - 100%)
     */
    public static final int PROB_DELETE_WITH_FREQUENTFLYER_ID_STR = 20;

    /**
     * Probability that is a seat is initially occupied (0% - 100%)
     */
    public static final int PROB_SEAT_OCCUPIED = 1; // 25;

    /**
     * Probability that UpdateCustomer should update FrequentFlyer records
     */
    public static final int PROB_UPDATE_FREQUENT_FLYER = 25;

    /**
     * Probability that a new Reservation will be added to the DeleteReservation queue
     */
    public static final int PROB_DELETE_RESERVATION = 50;

    /**
     * Probability that a new Reservation will be added to the UpdateReservation queue
     */
    public static final int PROB_UPDATE_RESERVATION = 50;

    /**
     * Probability that a deleted Reservation will be requeued for another NewReservation call
     */
    public static final int PROB_REQUEUE_DELETED_RESERVATION = 90;

    /**
     * Probability that FindFlights will use the distance search
     */
    public static final int PROB_FIND_FLIGHTS_NEARBY_AIRPORT = 25;

    /**
     * Probability that FindFlights will use two random airports as its input
     */
    public static final int PROB_FIND_FLIGHTS_RANDOM_AIRPORTS = 10;

    // ----------------------------------------------------------------
    // TIME CONSTANTS
    // ----------------------------------------------------------------

    /**
     * Number of microseconds in a day
     */
    public static final long MILLISECONDS_PER_MINUTE = 60000L; // 60sec * 1,000

    /**
     * Number of microseconds in a day
     */
    public static final long MILLISECONDS_PER_DAY = 86400000L; // 60sec * 60min * 24hr * 1,000

    /**
     * The format of the time codes used in HISTOGRAM_FLIGHTS_PER_DEPART_TIMES
     */
    public static final Pattern TIMECODE_PATTERN = Pattern.compile("([\\d]{2,2}):([\\d]{2,2})");

    // ----------------------------------------------------------------
    // CACHE SIZES
    // ----------------------------------------------------------------

    /**
     * The number of FlightIds we want to keep cached locally at a client
     */
    public static final int CACHE_LIMIT_FLIGHT_IDS = 10000;

    public static final int CACHE_LIMIT_PENDING_INSERTS = 10000;
    public static final int CACHE_LIMIT_PENDING_UPDATES = 5000;
    public static final int CACHE_LIMIT_PENDING_DELETES = 5000;

    // ----------------------------------------------------------------
    // DATA SET INFORMATION
    // ----------------------------------------------------------------

    /**
     * Table Names
     */
    public static final String TABLENAME_COUNTRY = "country";
    public static final String TABLENAME_AIRLINE = "airline";
    public static final String TABLENAME_CUSTOMER = "customer";
    public static final String TABLENAME_FREQUENT_FLYER = "frequent_flyer";
    public static final String TABLENAME_AIRPORT = "airport";
    public static final String TABLENAME_AIRPORT_DISTANCE = "airport_distance";
    public static final String TABLENAME_FLIGHT = "flight";
    public static final String TABLENAME_RESERVATION = "reservation";

    public static final String TABLENAME_CONFIG_PROFILE = "config_profile";
    public static final String TABLENAME_CONFIG_HISTOGRAMS = "config_histograms";

    /**
     * Histogram Data Set Names
     */
    public static final String HISTOGRAM_FLIGHTS_PER_AIRPORT = "flights_per_airport";
    public static final String HISTOGRAM_FLIGHTS_PER_DEPART_TIMES = "flights_per_time";

    /**
     * Tables that are loaded from data files
     */
    public static final String[] TABLES_DATAFILES = {
            SEATSConstants.TABLENAME_COUNTRY,
            SEATSConstants.TABLENAME_AIRPORT,
            SEATSConstants.TABLENAME_AIRLINE,
    };

    /**
     * Tables generated from random data
     * IMPORTANT: FLIGHT must come before FREQUENT_FLYER
     */
    public static final String[] TABLES_SCALING = {
            SEATSConstants.TABLENAME_CUSTOMER,
            SEATSConstants.TABLENAME_AIRPORT_DISTANCE,
            SEATSConstants.TABLENAME_FLIGHT,
            SEATSConstants.TABLENAME_FREQUENT_FLYER,
            SEATSConstants.TABLENAME_RESERVATION,
    };

    /**
     * Configuration Tables
     */
    public static final String[] TABLES_CONFIG = {
            SEATSConstants.TABLENAME_CONFIG_PROFILE,
            SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS,
    };

    /**
     * Histograms generated from data files
     */
    public static final String[] HISTOGRAM_DATA_FILES = {
            SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT,
            SEATSConstants.HISTOGRAM_FLIGHTS_PER_DEPART_TIMES,
    };

    /**
     * Tuple Code to Tuple Id Mapping
     * For some tables, we want to store a unique code that can be used to map
     * to the id of a tuple. Any table that has a foreign key reference to this table
     * will use the unique code in the input data tables instead of the id. Thus, we need
     * to keep a table of how to map these codes to the ids when loading.
     */

    public static final String AIRPORT_ID = "ap_id";
    public static final String AIRLINE_ID = "al_id";
    public static final String COUNTRY_ID = "co_id";
    public static final String AIRLINE_IATA_CODE = "al_iata_code";
    public static final String AIRPORT_CODE = "ap_code";
    public static final String COUNTRY_CODE = "co_code_3";

    public static final String[][] CODE_TO_ID_COLUMNS = {
            {TABLENAME_COUNTRY, COUNTRY_CODE, COUNTRY_ID},
            {TABLENAME_AIRPORT, AIRPORT_CODE, AIRPORT_ID},
            {TABLENAME_AIRLINE, AIRLINE_IATA_CODE, AIRLINE_ID},
    };

}
