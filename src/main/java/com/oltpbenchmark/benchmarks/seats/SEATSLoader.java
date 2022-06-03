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

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.seats.util.*;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.*;
import com.oltpbenchmark.util.RandomDistribution.Flat;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.RandomDistribution.Gaussian;
import com.oltpbenchmark.util.RandomDistribution.Zipf;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.collections4.set.ListOrderedSet;

import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

public class SEATSLoader extends Loader<SEATSBenchmark> {
    // -----------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // -----------------------------------------------------------------

    protected final SEATSProfile profile;

    /**
     * Mapping from Airports to their geolocation coordinates AirportCode ->
     * <Latitude, Longitude>
     */
    private final ListOrderedMap<String, Pair<Double, Double>> airport_locations = new ListOrderedMap<>();

    /**
     * AirportCode -> Set<AirportCode, Distance> Only store the records for
     * those airports in HISTOGRAM_FLIGHTS_PER_AIRPORT
     */
    private final Map<String, Map<String, Short>> airport_distances = new HashMap<>();

    /**
     * Store a list of FlightIds and the number of seats remaining for a
     * particular flight.
     */
    private final ListOrderedMap<FlightId, Short> seats_remaining = new ListOrderedMap<>();

    /**
     * Counter for the number of tables that we have finished loading
     */
    private final AtomicInteger finished = new AtomicInteger(0);

    /**
     * A histogram of the number of flights in the database per airline code
     */
    private final Histogram<String> flights_per_airline = new Histogram<>(true);

    private final RandomGenerator rng; // FIXME

    // -----------------------------------------------------------------
    // INITIALIZATION
    // -----------------------------------------------------------------

    public SEATSLoader(SEATSBenchmark benchmark) {
        super(benchmark);

        this.rng = benchmark.getRandomGenerator();
        // TODO: Sync with the base class rng
        this.profile = new SEATSProfile(benchmark, this.rng);

        if (LOG.isDebugEnabled()) {
            LOG.debug("CONSTRUCTOR: {}", SEATSLoader.class.getName());
        }
    }

    // -----------------------------------------------------------------
    // LOADING METHODS
    // -----------------------------------------------------------------

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();

        // High level locking overview, where step N+1 depends on step N
        // and latches are countDown()'d from top to bottom:
        //
        // 1. [histLatch] Histograms will be loaded on their own
        //
        // FIXED TABLES [fixedLatch]
        // 2.
        // [countryLatch] Country will be loaded on their own
        // AIRPORT depends on COUNTRY
        // AIRLINE depends on COUNTRY
        //
        // 3. [scalePrepLatch]
        // We need to load fixed table data into histograms before we
        // start to load scaling tables
        //
        // SCALING TABLES
        // 4.
        // [custLatch] CUSTOMER depends on AIRPORT
        // [distanceLatch] AIRPORT_DISTANCE depends on AIRPORT
        // [flightLatch] FLIGHT depends on AIRLINE, AIRPORT, AIRPORT_DISTANCE
        //
        // 5. [loadLatch]
        // RESERVATIONS depends on FLIGHT, CUSTOMER
        // FREQUENT_FLYER depends on FLIGHT, CUSTOMER, AIRLINE
        //
        // Important note: FLIGHT must come before FREQUENT_FLYER so that
        // we can use the flights_per_airline histogram when
        // selecting an airline to create a new FREQUENT_FLYER
        // account for a CUSTOMER
        //
        // 6. Then we save the profile

        final CountDownLatch histLatch = new CountDownLatch(1);

        // 1. [histLatch] HISTOGRAMS
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                loadHistograms();
            }

            @Override
            public void afterLoad() {
                histLatch.countDown();
            }
        });

        final CountDownLatch fixedLatch = new CountDownLatch(3);
        final CountDownLatch countryLatch = new CountDownLatch(1);

        // 2. [countryLatch] COUNTRY
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                loadFixedTable(conn, SEATSConstants.TABLENAME_COUNTRY);
            }

            @Override
            public void beforeLoad() {
                try {
                    histLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterLoad() {
                fixedLatch.countDown();
                countryLatch.countDown();
            }
        });

        // 2. AIRPORT depends on COUNTRY
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                loadFixedTable(conn, SEATSConstants.TABLENAME_AIRPORT);
            }

            @Override
            public void beforeLoad() {
                try {
                    countryLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterLoad() {
                fixedLatch.countDown();
            }
        });

        // 2. AIRLINE depends on COUNTRY
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                loadFixedTable(conn, SEATSConstants.TABLENAME_AIRLINE);
            }

            @Override
            public void beforeLoad() {
                try {
                    countryLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }

            @Override
            public void afterLoad() {
                fixedLatch.countDown();
            }
        });

        final CountDownLatch scalingPrepLatch = new CountDownLatch(1);

        // 3. [scalingPrepLatch] guards all of the fixed tables and should
        // be used from this point onwards instead of individual fixed locks
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                // Setup the # of flights per airline
                flights_per_airline.putAll(SEATSLoader.this.profile.getAirlineCodes(), 0);
            }

            @Override
            public void beforeLoad() {
                try {
                    fixedLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterLoad() {
                scalingPrepLatch.countDown();
            }
        });

        final CountDownLatch custLatch = new CountDownLatch(1);
        final CountDownLatch distanceLatch = new CountDownLatch(1);
        final CountDownLatch flightLatch = new CountDownLatch(1);

        // 4. [custLatch] CUSTOMER depends on AIRPORT
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {

                loadScalingTable(conn, SEATSConstants.TABLENAME_CUSTOMER);

            }

            @Override
            public void beforeLoad() {
                try {
                    scalingPrepLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }

            @Override
            public void afterLoad() {
                custLatch.countDown();
            }
        });

        // 4. AIRPORT_DISTANCE depends on AIRPORT
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {


                loadScalingTable(conn, SEATSConstants.TABLENAME_AIRPORT_DISTANCE);

            }

            @Override
            public void beforeLoad() {
                try {
                    scalingPrepLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterLoad() {
                distanceLatch.countDown();
            }
        });

        // 4. [flightLatch] FLIGHT depends on AIRPORT_DISTANCE, AIRLINE, AIRPORT
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {


                loadScalingTable(conn, SEATSConstants.TABLENAME_FLIGHT);

            }

            @Override
            public void beforeLoad() {
                try {
                    distanceLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterLoad() {
                flightLatch.countDown();
            }
        });

        final CountDownLatch loadLatch = new CountDownLatch(2);

        // 5. RESERVATIONS depends on FLIGHT, CUSTOMER
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {


                loadScalingTable(conn, SEATSConstants.TABLENAME_RESERVATION);

            }

            @Override
            public void beforeLoad() {
                try {
                    flightLatch.await();
                    custLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterLoad() {
                loadLatch.countDown();
            }
        });

        // 5. FREQUENT_FLYER depends on FLIGHT, CUSTOMER, AIRLINE
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {

                loadScalingTable(conn, SEATSConstants.TABLENAME_FREQUENT_FLYER);

            }

            @Override
            public void beforeLoad() {
                try {
                    flightLatch.await();
                    custLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }

            @Override
            public void afterLoad() {
                loadLatch.countDown();
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                profile.saveProfile(conn);
            }

            @Override
            public void beforeLoad() {
                try {
                    loadLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }

        });

        return threads;
    }

    /**
     * Load all the histograms used in the benchmark
     */
    protected void loadHistograms() {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Loading in %d histograms from files stored in '%s'", SEATSConstants.HISTOGRAM_DATA_FILES.length, this.profile.airline_data_dir));
        }

        // Now load in the histograms that we will need for generating the
        // flight data
        for (String histogramName : SEATSConstants.HISTOGRAM_DATA_FILES) {
            if (this.profile.histograms.containsKey(histogramName)) {
                LOG.warn("Already loaded histogram '{}'. Skipping...", histogramName);
                continue;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Loading in histogram data file for '{}'", histogramName);
            }
            Histogram<String> hist = null;

            try {
                // The Flights_Per_Airport histogram is actually a serialized
                // map that has a histogram
                // of the departing flights from each airport to all the others
                if (histogramName.equals(SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT)) {
                    Map<String, Histogram<String>> m = SEATSHistogramUtil.loadAirportFlights(this.profile.airline_data_dir);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Loaded %d airport flight histograms", m.size()));
                    }

                    // Store the airport codes information
                    this.profile.airport_histograms.putAll(m);

                    // We then need to flatten all of the histograms in this map
                    // into a single histogram that just counts the number of
                    // departing flights per airport. We will use this
                    // to get the distribution of where Customers are located
                    hist = new Histogram<>();
                    for (Entry<String, Histogram<String>> e : m.entrySet()) {
                        hist.put(e.getKey(), e.getValue().getSampleCount());
                    }

                    // All other histograms are just serialized and can be
                    // loaded directly
                } else {
                    hist = SEATSHistogramUtil.loadHistogram(histogramName, this.profile.airline_data_dir, true);
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load histogram '" + histogramName + "'", ex);
            }

            this.profile.histograms.put(histogramName, hist);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Loaded histogram '%s' [sampleCount=%d, valueCount=%d]", histogramName, hist.getSampleCount(), hist.getValueCount()));
            }
        }

    }

    /**
     * The fixed tables are those that are generated from the static data files
     * The number of tuples in these tables will not change based on the scale
     * factor.
     *
     * @param conn
     * @param table_name
     */
    protected void loadFixedTable(Connection conn, String table_name) {
        LOG.debug(String.format("Loading table '%s' from fixed file", table_name));
        try {
            Table catalog_tbl = this.benchmark.getCatalog().getTable(table_name);
            try (FixedDataIterable iterable = this.getFixedIterable(catalog_tbl)) {
                this.loadTable(conn, catalog_tbl, iterable, workConf.getBatchSize());
            }

        } catch (Throwable ex) {
            throw new RuntimeException("Failed to load data files for fixed-sized table '" + table_name + "'", ex);
        }
    }

    /**
     * The scaling tables are things that we will scale the number of tuples
     * based on the given scaling factor at runtime
     *
     * @param conn
     * @param table_name
     */
    protected void loadScalingTable(Connection conn, String table_name) {
        try {
            Table catalog_tbl = this.benchmark.getCatalog().getTable(table_name);
            Iterable<Object[]> iterable = this.getScalingIterable(catalog_tbl);
            this.loadTable(conn, catalog_tbl, iterable, workConf.getBatchSize());
        } catch (Throwable ex) {
            throw new RuntimeException("Failed to load data files for scaling-sized table '" + table_name + "'", ex);
        }
    }

    /**
     * @param catalog_tbl
     */
    public void loadTable(Connection conn, Table catalog_tbl, Iterable<Object[]> iterable, int batch_size) {
        // Special Case: Airport Locations
        final boolean is_airport = catalog_tbl.getName().equalsIgnoreCase(SEATSConstants.TABLENAME_AIRPORT);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Generating new records for table %s [batchSize=%d]", catalog_tbl.getName().toLowerCase(), batch_size));
        }
        final List<Column> columns = catalog_tbl.getColumns();

        // Check whether we have any special mappings that we need to maintain
        Map<Integer, Integer> code_2_id = new HashMap<>();
        Map<Integer, Map<String, Long>> mapping_columns = new HashMap<>();
        for (int col_code_idx = 0, cnt = columns.size(); col_code_idx < cnt; col_code_idx++) {
            Column catalog_col = columns.get(col_code_idx);
            String col_name = catalog_col.getName().toLowerCase();

            // Code Column -> Id Column Mapping
            // Check to see whether this table has columns that we need to map
            // their
            // code values to tuple ids
            String col_id_name = this.profile.code_columns.get(col_name);
            if (col_id_name != null) {
                Column catalog_id_col = catalog_tbl.getColumnByName(col_id_name);

                int col_id_idx = catalog_tbl.getColumnIndex(catalog_id_col);
                code_2_id.put(col_code_idx, col_id_idx);
            }

            // Foreign Key Column to Code->Id Mapping
            // If this columns references a foreign key that is used in the
            // Code->Id mapping that we generating above,
            // then we need to know when we should change the
            // column value from a code to the id stored in our lookup table
            if (this.profile.fkey_value_xref.containsKey(col_name)) {
                String col_fkey_name = this.profile.fkey_value_xref.get(col_name);
                mapping_columns.put(col_code_idx, this.profile.code_id_xref.get(col_fkey_name));
            }
        }

        int row_idx = 0;
        int row_batch = 0;

        String insert_sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        try (PreparedStatement insert_stmt = conn.prepareStatement(insert_sql)) {
            int[] sqlTypes = catalog_tbl.getColumnTypes();

            for (Object[] tuple : iterable) {
                // AIRPORT
                if (is_airport) {
                    // Skip any airport that does not have flights
                    int col_code_idx = catalog_tbl.getColumnByName("AP_CODE").getIndex();
                    if (!this.profile.hasFlights((String) tuple[col_code_idx])) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace(String.format("Skipping AIRPORT '%s' because it does not have any flights", tuple[col_code_idx]));
                        }
                        continue;
                    }

                    // Update the row # so that it matches
                    // what we're actually loading
                    int col_id_idx = catalog_tbl.getColumnByName("AP_ID").getIndex();
                    tuple[col_id_idx] = (long) (row_idx + 1);

                    // Store Locations
                    int col_lat_idx = catalog_tbl.getColumnByName("AP_LATITUDE").getIndex();
                    int col_lon_idx = catalog_tbl.getColumnByName("AP_LONGITUDE").getIndex();

                    // HACK: We need to cast floats to doubles for SQLite
                    Pair<Double, Double> coords;
                    if (tuple[col_lat_idx] instanceof Float) {
                        coords = Pair.of(Double.valueOf(tuple[col_lat_idx].toString()), Double.valueOf(tuple[col_lon_idx].toString()));
                    } else {
                        coords = Pair.of((Double) tuple[col_lat_idx], (Double) tuple[col_lon_idx]);
                    }
                    if (coords.first == null || coords.second == null) {
                        LOG.error(Arrays.toString(tuple));
                    }


                    this.airport_locations.put(tuple[col_code_idx].toString(), coords);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(String.format("Storing location for '%s': %s", tuple[col_code_idx], coords));
                    }
                }

                // Code Column -> Id Column
                for (int col_code_idx : code_2_id.keySet()) {

                    String code = tuple[col_code_idx].toString().trim();
                    if (code.length() > 0) {
                        Column from_column = columns.get(col_code_idx);

                        Column to_column = columns.get(code_2_id.get(col_code_idx));

                        long id = (Long) tuple[code_2_id.get(col_code_idx)];
                        if (LOG.isTraceEnabled()) {
                            LOG.trace(String.format("Mapping %s '%s' -> %s '%d'", from_column.getName().toLowerCase(), code, to_column.getName().toLowerCase(), id));
                        }
                        this.profile.code_id_xref.get(to_column.getName().toLowerCase()).put(code, id);
                    }
                }

                // Foreign Key Code -> Foreign Key Id
                for (int col_code_idx : mapping_columns.keySet()) {
                    if (tuple[col_code_idx] != null) {
                        String code = tuple[col_code_idx].toString();
                        tuple[col_code_idx] = mapping_columns.get(col_code_idx).get(code);
                    }
                }

                for (int i = 0; i < tuple.length; i++) {
                    try {
                        if (tuple[i] != null) {
                            insert_stmt.setObject(i + 1, tuple[i]);
                        } else {
                            insert_stmt.setNull(i + 1, sqlTypes[i]);
                        }
                    } catch (SQLDataException ex) {
                        LOG.error("INVALID {} TUPLE: {}", catalog_tbl.getName().toLowerCase(), Arrays.toString(tuple));
                        throw new RuntimeException("Failed to set value for " + catalog_tbl.getColumn(i).getName().toLowerCase(), ex);
                    }
                }
                insert_stmt.addBatch();
                row_idx++;

                if (++row_batch >= batch_size) {
                    LOG.trace(String.format("Loading %s batch [total=%d]", catalog_tbl.getName().toLowerCase(), row_idx));
                    insert_stmt.executeBatch();
                    insert_stmt.clearBatch();
                    row_batch = 0;
                }

            }

            if (row_batch > 0) {
                insert_stmt.executeBatch();
            }

        } catch (Exception ex) {
            throw new RuntimeException("Failed to load table " + catalog_tbl.getName().toLowerCase(), ex);
        }

        // Record the number of tuples that we loaded for this table in the
        // profile
        if (catalog_tbl.getName().equalsIgnoreCase(SEATSConstants.TABLENAME_RESERVATION)) {
            this.profile.num_reservations = row_idx + 1;
        }

        LOG.debug(String.format("Finished loading all %d tuples for %s", row_idx, catalog_tbl.getName().toLowerCase()));
    }

    // ----------------------------------------------------------------
    // FIXED-SIZE TABLE DATA GENERATION
    // ----------------------------------------------------------------

    /**
     * @param catalog_tbl
     * @return
     * @throws Exception
     */
    protected FixedDataIterable getFixedIterable(Table catalog_tbl) throws Exception {
        String path = SEATSBenchmark.getTableDataFilePath(this.profile.airline_data_dir, catalog_tbl);
        return new FixedDataIterable(catalog_tbl, path);
    }

    /**
     * Wrapper around TableDataIterable that will populate additional random
     * fields
     */
    protected class FixedDataIterable extends TableDataIterable {
        private final Set<Integer> rnd_string = new HashSet<>();
        private final Map<Integer, Integer> rnd_string_min = new HashMap<>();
        private final Map<Integer, Integer> rnd_string_max = new HashMap<>();
        private final Set<Integer> rnd_integer = new HashSet<>();

        public FixedDataIterable(Table catalog_tbl, String filePath) throws Exception {
            super(catalog_tbl, filePath, true, true);

            // Figure out which columns are random integers and strings
            for (Column catalog_col : catalog_tbl.getColumns()) {
                int col_idx = catalog_col.getIndex();
                if (catalog_col.getName().toUpperCase().contains("_SATTR")) {
                    this.rnd_string.add(col_idx);
                    this.rnd_string_min.put(col_idx, SEATSLoader.this.rng.nextInt(catalog_col.getSize() - 1));
                    this.rnd_string_max.put(col_idx, catalog_col.getSize());
                } else if (catalog_col.getName().toUpperCase().contains("_IATTR")) {
                    this.rnd_integer.add(catalog_col.getIndex());
                }
            }
        }

        @Override
        public Iterator<Object[]> iterator() {
            // This is nasty old boy!
            return (new TableDataIterable.TableIterator() {

                @Override
                public Object[] next() {
                    Object[] tuple = super.next();

                    // Random String (*_SATTR##)
                    for (int col_idx : FixedDataIterable.this.rnd_string) {
                        int min_length = FixedDataIterable.this.rnd_string_min.get(col_idx);
                        int max_length = FixedDataIterable.this.rnd_string_max.get(col_idx);
                        tuple[col_idx] = SEATSLoader.this.rng.astring(min_length, max_length);
                    }
                    // Random Integer (*_IATTR##)
                    for (int col_idx : FixedDataIterable.this.rnd_integer) {
                        tuple[col_idx] = SEATSLoader.this.rng.nextLong();
                    }

                    return (tuple);
                }
            });
        }
    }

    // ----------------------------------------------------------------
    // SCALING TABLE DATA GENERATION
    // ----------------------------------------------------------------

    /**
     * Return an iterable that spits out tuples for scaling tables
     *
     * @param catalog_tbl the target table that we need an iterable for
     */
    protected Iterable<Object[]> getScalingIterable(Table catalog_tbl) {
        String name = catalog_tbl.getName().toLowerCase();
        ScalingDataIterable it = null;
        double scaleFactor = this.workConf.getScaleFactor();
        long num_customers = Math.round(SEATSConstants.CUSTOMERS_COUNT * scaleFactor);

        // Customers
        if (name.equalsIgnoreCase(SEATSConstants.TABLENAME_CUSTOMER)) {
            it = new CustomerIterable(catalog_tbl, num_customers);
        }
        // FrequentFlyer
        else if (name.equalsIgnoreCase(SEATSConstants.TABLENAME_FREQUENT_FLYER)) {
            it = new FrequentFlyerIterable(catalog_tbl, num_customers);
        }
        // Airport Distance
        else if (name.equalsIgnoreCase(SEATSConstants.TABLENAME_AIRPORT_DISTANCE)) {
            int max_distance = Integer.MAX_VALUE; // SEATSConstants.DISTANCES[SEATSConstants.DISTANCES.length
            // - 1];
            it = new AirportDistanceIterable(catalog_tbl, max_distance);
        }
        // Flights
        else if (name.equalsIgnoreCase(SEATSConstants.TABLENAME_FLIGHT)) {
            it = new FlightIterable(catalog_tbl, (int) Math.round(SEATSConstants.FLIGHTS_DAYS_PAST * scaleFactor), (int) Math.round(SEATSConstants.FLIGHTS_DAYS_FUTURE * scaleFactor));
        }
        // Reservations
        else if (name.equalsIgnoreCase(SEATSConstants.TABLENAME_RESERVATION)) {
            long total = Math.round((SEATSConstants.FLIGHTS_PER_DAY_MIN + SEATSConstants.FLIGHTS_PER_DAY_MAX) / 2d * scaleFactor);
            it = new ReservationIterable(catalog_tbl, total);
        }

        return (it);
    }

    /**
     * Base Iterable implementation for scaling tables Sub-classes implement the
     * specialValue() method to generate values of a specific type instead of
     * just using the random data generators
     */
    protected abstract class ScalingDataIterable implements Iterable<Object[]> {
        private final Table catalog_tbl;
        private final boolean[] special;
        private final Object[] data;
        private final int[] types;
        protected long total;
        private long last_id = 0;

        /**
         * @param catalog_tbl
         * @param total
         * @param special_columns The offsets of the columns that we will invoke
         *                        specialValue() to get their values
         * @throws Exception
         */
        public ScalingDataIterable(Table catalog_tbl, long total, int[] special_columns) {
            this.catalog_tbl = catalog_tbl;
            this.total = total;
            this.data = new Object[this.catalog_tbl.getColumns().size()];
            this.special = new boolean[this.catalog_tbl.getColumns().size()];

            for (int i = 0; i < this.special.length; i++) {
                this.special[i] = false;
            }
            for (int idx : special_columns) {
                this.special[idx] = true;
            }

            // Cache the types
            this.types = new int[catalog_tbl.getColumns().size()];
            for (Column catalog_col : catalog_tbl.getColumns()) {
                this.types[catalog_col.getIndex()] = catalog_col.getType();
            }
        }

        /**
         * Generate a special value for this particular column index
         *
         * @param id
         * @param column_idx
         * @return
         */
        protected abstract Object specialValue(long id, int column_idx);

        /**
         * Simple callback when the ScalingDataIterable is finished
         */
        protected void callbackFinished() {
            // Nothing...
        }

        protected boolean hasNext() {
            boolean has_next = (this.last_id < this.total);
            if (!has_next) {
                this.callbackFinished();
            }
            return (has_next);
        }

        /**
         * Generate the iterator
         */
        @Override
        public Iterator<Object[]> iterator() {
            return (new Iterator<Object[]>() {
                @Override
                public boolean hasNext() {
                    return (ScalingDataIterable.this.hasNext());
                }

                @Override
                public Object[] next() {
                    // For every column for this table, generate the random data that
                    // we need to populate for the new tuple that we will return with next()
                    for (int i = 0; i < ScalingDataIterable.this.data.length; i++) {
                        Column catalog_col = ScalingDataIterable.this.catalog_tbl.getColumn(i);

                        // Special Value Column
                        if (ScalingDataIterable.this.special[i]) {
                            ScalingDataIterable.this.data[i] = ScalingDataIterable.this.specialValue(ScalingDataIterable.this.last_id, i);

                        // Id column (always first unless overridden in
                        // special)
                        } else if (i == 0) {
                            ScalingDataIterable.this.data[i] = ScalingDataIterable.this.last_id;

                        // Strings
                        } else if (SQLUtil.isStringType(ScalingDataIterable.this.types[i])) {
                            // WARN: If you ever have problems with running out of memory because of this block
                            // of code here, check that the length of the column from the catalog matches the DDL.
                            // SQLite incorrectly reports that the size of the column was massive for the customer
                            // table, so then we would allocate 2GB strings.
                            int max_len = catalog_col.getSize();
                            int min_len = SEATSLoader.this.rng.nextInt(max_len - 1);
                            ScalingDataIterable.this.data[i] = SEATSLoader.this.rng.astring(min_len, max_len);

                        // Ints/Longs
                        } else {
                            ScalingDataIterable.this.data[i] = SEATSLoader.this.rng.number(0, 1 << 30);
                        }
                    }
                    ScalingDataIterable.this.last_id++;
                    return (ScalingDataIterable.this.data);
                }

                @Override
                public void remove() {
                    // Not Implemented
                }
            });
        }
    }

    // ----------------------------------------------------------------
    // CUSTOMERS
    // ----------------------------------------------------------------
    protected class CustomerIterable extends ScalingDataIterable {
        private final FlatHistogram<String> rand;
        private final RandomDistribution.Flat randBalance;
        private String airport_code = null;
        private CustomerId last_id = null;

        public CustomerIterable(Table catalog_tbl, long total) {
            super(catalog_tbl, total, new int[]{0, 1, 2, 3});

            // Use the flights per airport histogram to select where people are
            // located
            Histogram<String> histogram = SEATSLoader.this.profile.getHistogram(SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT);
            this.rand = new FlatHistogram<>(SEATSLoader.this.rng, histogram);
            if (LOG.isDebugEnabled()) {
                this.rand.enableHistory();
            }

            this.randBalance = new RandomDistribution.Flat(SEATSLoader.this.rng, 1000, 10000);
        }

        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // CUSTOMER ID
                case (0): {
                    // HACK: The flights_per_airport histogram may not match up
                    // exactly with the airport
                    // data files, so we'll just spin until we get a good one
                    Long airport_id = null;
                    while (airport_id == null) {
                        this.airport_code = this.rand.nextValue();
                        airport_id = SEATSLoader.this.profile.getAirportId(this.airport_code);
                    }
                    int next_customer_id = SEATSLoader.this.profile.incrementAirportCustomerCount(airport_id);
                    this.last_id = new CustomerId(next_customer_id, airport_id);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("NEW CUSTOMER: {} / {}", this.last_id.encode(), this.last_id);
                    }
                    value = this.last_id.encode();
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} => {} [{}]", value, this.airport_code, SEATSLoader.this.profile.getCustomerIdCount(airport_id));
                    }
                    break;
                }
                // CUSTOMER ID STR
                case (1): {

                    value = this.last_id.encode();
                    this.last_id = null;
                    break;
                }
                // LOCAL AIRPORT
                case (2): {

                    value = this.airport_code;
                    break;
                }
                // BALANCE
                case (3): {
                    value = (double) this.randBalance.nextInt();
                    break;
                }
                // BAD MOJO!
                default:

            }
            return (value);
        }

        @Override
        protected void callbackFinished() {
            if (LOG.isTraceEnabled()) {
                Histogram<String> h = this.rand.getHistogramHistory();
                LOG.trace(String.format("Customer Local Airports Histogram [valueCount=%d, sampleCount=%d]\n%s", h.getValueCount(), h.getSampleCount(), h));
            }
        }
    }

    // ----------------------------------------------------------------
    // FREQUENT_FLYER
    // ----------------------------------------------------------------
    protected class FrequentFlyerIterable extends ScalingDataIterable {
        private final Iterator<CustomerId> customer_id_iterator;
        private final short[] ff_per_customer;
        private final FlatHistogram<String> airline_rand;

        private int customer_idx = 0;
        private CustomerId last_customer_id = null;
        private final Collection<String> customer_airlines = new HashSet<>();

        public FrequentFlyerIterable(Table catalog_tbl, long num_customers) {
            super(catalog_tbl, num_customers, new int[]{0, 1, 2});

            this.customer_id_iterator = new CustomerIdIterable(SEATSLoader.this.profile.airport_max_customer_id).iterator();
            this.last_customer_id = this.customer_id_iterator.next();

            // A customer is more likely to have a FREQUENT_FLYER account with
            // an airline that has more flights.
            // IMPORTANT: Add one to all of the airlines so that we don't get
            // trapped
            // in an infinite loop

            SEATSLoader.this.flights_per_airline.putAll();
            this.airline_rand = new FlatHistogram<>(SEATSLoader.this.rng, SEATSLoader.this.flights_per_airline);
            if (LOG.isTraceEnabled()) {
                this.airline_rand.enableHistory();
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Flights Per Airline:\n{}", SEATSLoader.this.flights_per_airline);
            }

            // Loop through for the total customers and figure out how many
            // entries we
            // should have for each one. This will be our new total;
            long max_per_customer = Math.min(Math.round(SEATSConstants.CUSTOMER_NUM_FREQUENTFLYERS_MAX * Math.max(1, SEATSLoader.this.scaleFactor)), SEATSLoader.this.flights_per_airline.getValueCount());
            Zipf ff_zipf = new Zipf(SEATSLoader.this.rng, SEATSConstants.CUSTOMER_NUM_FREQUENTFLYERS_MIN, max_per_customer, SEATSConstants.CUSTOMER_NUM_FREQUENTFLYERS_SIGMA);
            long new_total = 0;
            long total = SEATSLoader.this.profile.getCustomerIdCount();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Num of Customers: {}", total);
            }
            this.ff_per_customer = new short[(int) total];
            for (int i = 0; i < total; i++) {
                this.ff_per_customer[i] = (short) ff_zipf.nextInt();
                if (this.ff_per_customer[i] > max_per_customer) {
                    this.ff_per_customer[i] = (short) max_per_customer;
                }
                new_total += this.ff_per_customer[i];
            }
            this.total = new_total;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Constructing {} FrequentFlyer tuples...", this.total);
            }
        }

        @Override
        protected Object specialValue(long id, int columnIdx) {
            String value = null;
            switch (columnIdx) {
                // CUSTOMER ID
                case (0): {
                    while (this.customer_idx < this.ff_per_customer.length && this.ff_per_customer[this.customer_idx] <= 0) {
                        this.customer_idx++;
                        this.customer_airlines.clear();
                        if (LOG.isTraceEnabled()) {
                            LOG.trace(String.format("CUSTOMER IDX: %d / %d", this.customer_idx, SEATSLoader.this.profile.getCustomerIdCount()));
                        }

                        this.last_customer_id = this.customer_id_iterator.next();
                    }
                    this.ff_per_customer[this.customer_idx]--;
                    value = this.last_customer_id.encode();
                    break;
                }
                // AIRLINE ID
                case (1): {

                    do {
                        value = this.airline_rand.nextValue();
                    }
                    while (this.customer_airlines.contains(value));
                    this.customer_airlines.add(value);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} => {}", this.last_customer_id, value);
                    }
                    break;
                }
                // CUSTOMER_ID_STR
                case (2): {
                    value = this.last_customer_id.encode();
                    break;
                }
                // BAD MOJO!
                default:

            }
            return (value);
        }

        @Override
        protected void callbackFinished() {
            if (LOG.isTraceEnabled()) {
                Histogram<String> h = this.airline_rand.getHistogramHistory();
                LOG.trace(String.format("Airline Flights Histogram [valueCount=%d, sampleCount=%d]\n%s", h.getValueCount(), h.getSampleCount(), h));
            }
        }
    }

    // ----------------------------------------------------------------
    // AIRPORT_DISTANCE
    // ----------------------------------------------------------------
    protected class AirportDistanceIterable extends ScalingDataIterable {
        private final int max_distance;
        private final int num_airports;
        private final Collection<String> record_airports;

        private int outer_ctr = 0;
        private String outer_airport;
        private Pair<Double, Double> outer_location;

        private Integer last_inner_ctr = null;
        private String inner_airport;
        private Pair<Double, Double> inner_location;
        private double distance;

        /**
         * Constructor
         *
         * @param catalog_tbl
         * @param max_distance
         */
        public AirportDistanceIterable(Table catalog_tbl, int max_distance) {
            super(catalog_tbl, Long.MAX_VALUE, new int[]{0, 1, 2});
            // total work around ????
            this.max_distance = max_distance;
            this.num_airports = SEATSLoader.this.airport_locations.size();
            this.record_airports = SEATSLoader.this.profile.getAirportCodes();
        }

        /**
         * Find the next two airports that are within our max_distance limit. We
         * keep track of where we were in the inner loop using last_inner_ctr
         */
        @Override
        protected boolean hasNext() {
            for (; this.outer_ctr < (this.num_airports - 1); this.outer_ctr++) {
                this.outer_airport = SEATSLoader.this.airport_locations.get(this.outer_ctr);
                this.outer_location = SEATSLoader.this.airport_locations.getValue(this.outer_ctr);
                if (!SEATSLoader.this.profile.hasFlights(this.outer_airport)) {
                    continue;
                }

                int inner_ctr = (this.last_inner_ctr != null ? this.last_inner_ctr : this.outer_ctr + 1);
                this.last_inner_ctr = null;
                for (; inner_ctr < this.num_airports; inner_ctr++) {

                    this.inner_airport = SEATSLoader.this.airport_locations.get(inner_ctr);
                    this.inner_location = SEATSLoader.this.airport_locations.getValue(inner_ctr);
                    if (!SEATSLoader.this.profile.hasFlights(this.inner_airport)) {
                        continue;
                    }
                    this.distance = DistanceUtil.distance(this.outer_location, this.inner_location);

                    // Store the distance between the airports locally if either
                    // one is in our
                    // flights-per-airport data set
                    if (this.record_airports.contains(this.outer_airport) && this.record_airports.contains(this.inner_airport)) {
                        SEATSLoader.this.setDistance(this.outer_airport, this.inner_airport, this.distance);
                    }

                    // Stop here if these two airports are within range
                    if (this.distance > 0 && this.distance <= this.max_distance) {
                        // System.err.println(this.outer_airport + "->" +
                        // this.inner_airport + ": " + distance);
                        this.last_inner_ctr = inner_ctr + 1;
                        return (true);
                    }
                }
            }
            return (false);
        }

        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // OUTER AIRPORT
                case (0):
                    value = this.outer_airport;
                    break;
                // INNER AIRPORT
                case (1):
                    value = this.inner_airport;
                    break;
                // DISTANCE
                case (2):
                    value = this.distance;
                    break;
                // BAD MOJO!
                default:

            }
            return (value);
        }
    }

    // ----------------------------------------------------------------
    // FLIGHTS
    // ----------------------------------------------------------------
    protected class FlightIterable extends ScalingDataIterable {
        private final FlatHistogram<String> airlines;
        private final FlatHistogram<String> airports;
        private final Map<String, FlatHistogram<String>> flights_per_airport = new HashMap<>();
        private final FlatHistogram<String> flight_times;
        private final Flat prices;

        private final Set<FlightId> todays_flights = new HashSet<>();
        private final ListOrderedMap<Timestamp, Integer> flights_per_day = new ListOrderedMap<>();

        private int day_idx = 0;
        private final Timestamp today;
        private Timestamp start_date;

        private FlightId flight_id;
        private String depart_airport;
        private String arrive_airport;
        private String airline_code;
        private Long airline_id;
        private Timestamp depart_time;
        private Timestamp arrive_time;
        private int status;

        public FlightIterable(Table catalog_tbl, int days_past, int days_future) {
            super(catalog_tbl, Long.MAX_VALUE, new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});


            this.prices = new Flat(SEATSLoader.this.rng, SEATSConstants.RESERVATION_PRICE_MIN, SEATSConstants.RESERVATION_PRICE_MAX);

            // Flights per Airline
            Collection<String> all_airlines = SEATSLoader.this.profile.getAirlineCodes();
            Histogram<String> histogram = new Histogram<>();
            histogram.putAll(all_airlines);

            // Embed a Gaussian distribution
            Gaussian gauss_rng = new Gaussian(SEATSLoader.this.rng, 0, all_airlines.size());
            this.airlines = new FlatHistogram<>(gauss_rng, histogram);

            // Flights Per Airport
            histogram = SEATSLoader.this.profile.getHistogram(SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT);
            this.airports = new FlatHistogram<>(SEATSLoader.this.rng, histogram);
            for (String airport_code : histogram.values()) {
                histogram = SEATSLoader.this.profile.getFightsPerAirportHistogram(airport_code);

                this.flights_per_airport.put(airport_code, new FlatHistogram<>(SEATSLoader.this.rng, histogram));
            }

            // Flights Per Departure Time
            histogram = SEATSLoader.this.profile.getHistogram(SEATSConstants.HISTOGRAM_FLIGHTS_PER_DEPART_TIMES);
            this.flight_times = new FlatHistogram<>(SEATSLoader.this.rng, histogram);

            // Figure out how many flights that we want for each day
            this.today = new Timestamp(System.currentTimeMillis());

            // Sometimes there are more flights per day, and sometimes there are
            // fewer
            Gaussian gaussian = new Gaussian(SEATSLoader.this.rng, SEATSConstants.FLIGHTS_PER_DAY_MIN, SEATSConstants.FLIGHTS_PER_DAY_MAX);

            this.total = 0;
            boolean first = true;
            for (long t = this.today.getTime() - (days_past * SEATSConstants.MILLISECONDS_PER_DAY); t < this.today.getTime(); t += SEATSConstants.MILLISECONDS_PER_DAY) {
                Timestamp timestamp = new Timestamp(t);
                if (first) {
                    this.start_date = timestamp;
                    first = false;
                }
                int num_flights = gaussian.nextInt();
                this.flights_per_day.put(timestamp, num_flights);
                this.total += num_flights;
            }
            if (this.start_date == null) {
                this.start_date = this.today;
            }
            SEATSLoader.this.profile.setFlightStartDate(this.start_date);

            // This is for upcoming flights that we want to be able to schedule
            // new reservations for in the benchmark
            SEATSLoader.this.profile.setFlightUpcomingDate(this.today);
            for (long t = this.today.getTime(), last_date = this.today.getTime() + (days_future * SEATSConstants.MILLISECONDS_PER_DAY); t <= last_date; t += SEATSConstants.MILLISECONDS_PER_DAY) {
                Timestamp timestamp = new Timestamp(t);
                int num_flights = gaussian.nextInt();
                this.flights_per_day.put(timestamp, num_flights);
                this.total += num_flights;
            }

            // Update profile
            SEATSLoader.this.profile.setFlightPastDays(days_past);
            SEATSLoader.this.profile.setFlightFutureDays(days_future);
        }

        /**
         * Convert a time string "HH:MM" to a Timestamp object
         *
         * @param code
         * @return
         */
        private Timestamp convertTimeString(Timestamp base_date, String code) {
            Matcher m = SEATSConstants.TIMECODE_PATTERN.matcher(code);
            boolean result = m.find();


            int hour = -1;
            try {
                hour = Integer.valueOf(m.group(1));
            } catch (Throwable ex) {
                throw new RuntimeException("Invalid HOUR in time code '" + code + "'", ex);
            }


            int minute = -1;
            try {
                minute = Integer.valueOf(m.group(2));
            } catch (Throwable ex) {
                throw new RuntimeException("Invalid MINUTE in time code '" + code + "'", ex);
            }


            long offset = (hour * 60 * SEATSConstants.MILLISECONDS_PER_MINUTE) + (minute * SEATSConstants.MILLISECONDS_PER_MINUTE);
            return (new Timestamp(base_date.getTime() + offset));
        }

        /**
         * Select all the data elements for the current tuple
         *
         * @param date
         */
        private void populate(Timestamp date) {
            // Depart/Arrive Airports
            this.depart_airport = this.airports.nextValue();
            this.arrive_airport = this.flights_per_airport.get(this.depart_airport).nextValue();

            // Depart/Arrive Times
            this.depart_time = this.convertTimeString(date, this.flight_times.nextValue());
            this.arrive_time = SEATSLoader.this.calculateArrivalTime(this.depart_airport, this.arrive_airport, this.depart_time);

            // Airline
            this.airline_code = this.airlines.nextValue();
            this.airline_id = SEATSLoader.this.profile.getAirlineId(this.airline_code);

            // Status
            this.status = 0; // TODO

            this.flights_per_day.put(date, this.flights_per_day.get(date) - 1);
        }

        /**
         * Returns true if this seat is occupied (which means we must generate a
         * reservation)
         */
        boolean seatIsOccupied() {
            return (SEATSLoader.this.rng.nextInt(100) < SEATSConstants.PROB_SEAT_OCCUPIED);
        }

        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // FLIGHT ID
                case (0): {
                    // Figure out what date we are currently on
                    Integer remaining = null;
                    Timestamp date;
                    do {
                        // Move to the next day.
                        // Make sure that we reset the set of FlightIds that
                        // we've used for today
                        if (remaining != null) {
                            this.todays_flights.clear();
                            this.day_idx++;
                        }
                        date = this.flights_per_day.get(this.day_idx);
                        remaining = this.flights_per_day.getValue(this.day_idx);
                    }
                    while (remaining <= 0 && this.day_idx + 1 < this.flights_per_day.size());


                    // Keep looping until we get a FlightId that we haven't seen
                    // yet for this date
                    while (true) {
                        this.populate(date);

                        // Generate a composite FlightId
                        this.flight_id = new FlightId(this.airline_id, SEATSLoader.this.profile.getAirportId(this.depart_airport), SEATSLoader.this.profile.getAirportId(this.arrive_airport), this.start_date, this.depart_time);
                        if (!this.todays_flights.contains(this.flight_id)) {
                            break;
                        }
                    }
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(String.format("%s [remaining=%d, dayIdx=%d]", this.flight_id, remaining, this.day_idx));
                    }


                    this.todays_flights.add(this.flight_id);
                    SEATSLoader.this.addFlightId(this.flight_id);
                    value = this.flight_id.encode();
                    break;
                }
                // AIRLINE ID
                case (1): {
                    value = this.airline_code;
                    SEATSLoader.this.flights_per_airline.put(this.airline_code);
                    break;
                }
                // DEPART AIRPORT
                case (2): {
                    value = this.depart_airport;
                    break;
                }
                // DEPART TIME
                case (3): {
                    value = this.depart_time;
                    break;
                }
                // ARRIVE AIRPORT
                case (4): {
                    value = this.arrive_airport;
                    break;
                }
                // ARRIVE TIME
                case (5): {
                    value = this.arrive_time;
                    break;
                }
                // FLIGHT STATUS
                case (6): {
                    value = this.status;
                    break;
                }
                // BASE PRICE
                case (7): {
                    value = (double) this.prices.nextInt();
                    break;
                }
                // SEATS TOTAL
                case (8): {
                    value = SEATSConstants.FLIGHTS_NUM_SEATS;
                    break;
                }
                // SEATS REMAINING
                case (9): {
                    // We have to figure this out ahead of time since we need to
                    // populate the tuple now
                    for (int seatnum = 0; seatnum < SEATSConstants.FLIGHTS_NUM_SEATS; seatnum++) {
                        if (!this.seatIsOccupied()) {
                            continue;
                        }
                        SEATSLoader.this.decrementFlightSeat(this.flight_id);
                    }
                    value = (long) SEATSLoader.this.getFlightRemainingSeats(this.flight_id);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("{} SEATS REMAINING: {}", this.flight_id, value);
                    }
                    break;
                }
                // BAD MOJO!
                default:

            }
            return (value);
        }
    }

    // ----------------------------------------------------------------
    // RESERVATIONS
    // ----------------------------------------------------------------
    protected class ReservationIterable extends ScalingDataIterable {
        private final RandomDistribution.Flat prices = new RandomDistribution.Flat(SEATSLoader.this.rng, SEATSConstants.RESERVATION_PRICE_MIN, SEATSConstants.RESERVATION_PRICE_MAX);

        /**
         * For each airport id, store a list of ReturnFlight objects that
         * represent customers that need return flights back to their home
         * airport ArriveAirportId -> ReturnFlights
         */
        private final Map<Long, TreeSet<ReturnFlight>> airport_returns = new HashMap<>();

        /**
         * When this flag is true, then the data generation thread is finished
         */
        private boolean done = false;

        /**
         * We use a Gaussian distribution for determining how long a customer
         * will stay at their destination before needing to return to their
         * original airport
         */
        private final Gaussian rand_returns = new Gaussian(SEATSLoader.this.rng, SEATSConstants.CUSTOMER_RETURN_FLIGHT_DAYS_MIN, SEATSConstants.CUSTOMER_RETURN_FLIGHT_DAYS_MAX);

        private final LinkedBlockingDeque<Object[]> queue = new LinkedBlockingDeque<>(100);
        private Object[] current = null;
        private Throwable error = null;

        /**
         * Constructor
         *
         * @param catalog_tbl
         * @param total
         */
        public ReservationIterable(Table catalog_tbl, long total) {
            // Special Columns: R_C_ID, R_F_ID, R_F_AL_ID, R_SEAT, R_PRICE
            super(catalog_tbl, total, new int[]{1, 2, 3, 4});

            for (long airport_id : SEATSLoader.this.profile.getAirportIds()) {
                // Return Flights per airport
                this.airport_returns.put(airport_id, new TreeSet<>());
            }

            // Data Generation Thread
            // Ok, hang on tight. We are going to fork off a separate thread to
            // generate our tuples because it's easier than trying to pick up
            // where we left off every time. That means that when hasNext() is
            // called, it will block and poke this thread to start running.
            // Once this thread has generate a new tuple, it will block
            // itself and then poke the hasNext() thread. This is sort of
            // like a hacky version of Python's yield
            new Thread() {
                @Override
                public void run() {
                    try {
                        ReservationIterable.this.generateData();
                    } catch (Throwable ex) {
                        // System.err.println("Airport Customers:\n" +
                        // getAirportCustomerHistogram());
                        ReservationIterable.this.error = ex;
                    } finally {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Reservation generation thread is finished");
                        }
                        ReservationIterable.this.done = true;
                    }
                }
            }.start();
        }

        private void generateData() throws Exception {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reservation data generation thread started");
            }

            Collection<CustomerId> flight_customer_ids = new HashSet<>();
            Collection<ReturnFlight> returning_customers = new ListOrderedSet<>();

            // Loop through the flights and generate reservations
            for (FlightId flight_id : SEATSLoader.this.getFlightIds()) {
                long depart_airport_id = flight_id.getDepartAirportId();
                String depart_airport_code = SEATSLoader.this.profile.getAirportCode(depart_airport_id);
                long arrive_airport_id = flight_id.getArriveAirportId();
                String arrive_airport_code = SEATSLoader.this.profile.getAirportCode(arrive_airport_id);
                Timestamp depart_time = flight_id.getDepartDateAsTimestamp(SEATSLoader.this.profile.getFlightStartDate());
                Timestamp arrive_time = SEATSLoader.this.calculateArrivalTime(depart_airport_code, arrive_airport_code, depart_time);
                flight_customer_ids.clear();

                // For each flight figure out which customers are returning
                this.getReturningCustomers(returning_customers, flight_id);
                int booked_seats = SEATSConstants.FLIGHTS_NUM_SEATS - SEATSLoader.this.getFlightRemainingSeats(flight_id);

                if (LOG.isTraceEnabled()) {
                    Map<String, Object> m = new ListOrderedMap<>();
                    m.put("Flight Id", flight_id + " / " + flight_id.encode());
                    m.put("Departure", String.format("%s / %s", SEATSLoader.this.profile.getAirportCode(depart_airport_id), depart_time));
                    m.put("Arrival", String.format("%s / %s", SEATSLoader.this.profile.getAirportCode(arrive_airport_id), arrive_time));
                    m.put("Booked Seats", booked_seats);
                    m.put(String.format("Returning Customers[%d]", returning_customers.size()), StringUtil.join("\n", returning_customers));
                    LOG.trace("Flight Information\n{}", StringUtil.formatMaps(m));
                }

                for (int seatnum = 0; seatnum < booked_seats; seatnum++) {
                    CustomerId customer_id = null;
                    Integer airport_customer_cnt = SEATSLoader.this.profile.getCustomerIdCount(depart_airport_id);
                    boolean local_customer = airport_customer_cnt != null && (flight_customer_ids.size() < airport_customer_cnt);
                    int tries = 2000;
                    ReturnFlight return_flight = null;
                    while (tries > 0) {
                        return_flight = null;

                        // Always book returning customers first
                        if (!returning_customers.isEmpty()) {
                            return_flight = CollectionUtil.pop(returning_customers);
                            customer_id = return_flight.getCustomerId();
                        }
                        // New Outbound Reservation
                        // Prefer to use a customer based out of the local
                        // airport
                        else if (local_customer) {
                            customer_id = SEATSLoader.this.profile.getRandomCustomerId(depart_airport_id);
                        }
                        // New Outbound Reservation
                        // We'll take anybody!
                        else {
                            customer_id = SEATSLoader.this.profile.getRandomCustomerId();
                        }
                        if (!flight_customer_ids.contains(customer_id)) {
                            break;
                        }
                        tries--;
                    }


                    // If this is return flight, then there's nothing extra that
                    // we need to do
                    if (return_flight != null) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Booked return flight: {} [remaining={}]", return_flight, returning_customers.size());
                        }

                        // If it's a new outbound flight, then we will randomly
                        // decide when this customer will return (if at all)
                    } else {
                        if (SEATSLoader.this.rng.nextInt(100) < SEATSConstants.PROB_SINGLE_FLIGHT_RESERVATION) {
                            // Do nothing for now...

                            // Create a ReturnFlight object to record that this
                            // customer needs a flight
                            // back to their original depart airport
                        } else {
                            int return_days = this.rand_returns.nextInt();
                            return_flight = new ReturnFlight(customer_id, depart_airport_id, depart_time, return_days);
                            this.airport_returns.get(arrive_airport_id).add(return_flight);
                        }
                    }


                    flight_customer_ids.add(customer_id);

                    if (LOG.isTraceEnabled()) {
                        LOG.trace(String.format("New reservation ready. Adding to queue! [queueSize=%d]", this.queue.size()));
                    }
                    this.queue.put(new Object[]{customer_id, flight_id, seatnum});
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reservation data generation thread is finished");
            }
        }

        /**
         * Return a list of the customers that need to return to their original
         * location on this particular flight.
         *
         * @param flight_id
         * @return
         */
        private void getReturningCustomers(Collection<ReturnFlight> returning_customers, FlightId flight_id) {
            Timestamp flight_date = flight_id.getDepartDateAsTimestamp(SEATSLoader.this.profile.getFlightStartDate());
            returning_customers.clear();
            Set<ReturnFlight> returns = this.airport_returns.get(flight_id.getDepartAirportId());
            if (!returns.isEmpty()) {
                for (ReturnFlight return_flight : returns) {
                    if (return_flight.getReturnDate().compareTo(flight_date) > 0) {
                        break;
                    }
                    if (return_flight.getReturnAirportId() == flight_id.getArriveAirportId()) {
                        returning_customers.add(return_flight);
                    }
                }
                if (!returning_customers.isEmpty()) {
                    returns.removeAll(returning_customers);
                }
            }
        }

        @Override
        protected boolean hasNext() {
            if (LOG.isTraceEnabled()) {
                LOG.trace("hasNext() called");
            }
            this.current = null;
            while (!this.done || !this.queue.isEmpty()) {
                if (this.error != null) {
                    throw new RuntimeException("Failed to generate Reservation records", this.error);
                }

                try {
                    this.current = this.queue.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    throw new RuntimeException("Unexpected interruption!", ex);
                }
                if (this.current != null) {
                    return (true);
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("There were no new reservations. Let's try again!");
                }
            }
            return (false);
        }

        @Override
        protected Object specialValue(long id, int columnIdx) {

            Object value = null;
            switch (columnIdx) {
                // CUSTOMER ID
                case (1): {
                    value = ((CustomerId) this.current[0]).encode();
                    break;
                }
                // FLIGHT ID
                case (2): {
                    FlightId flight_id = (FlightId) this.current[1];
                    value = flight_id.encode();
                    if (SEATSLoader.this.profile.getReservationUpcomingOffset() == null && flight_id.isUpcoming(SEATSLoader.this.profile.getFlightStartDate(), SEATSLoader.this.profile.getFlightPastDays())) {
                        SEATSLoader.this.profile.setReservationUpcomingOffset(id);
                    }
                    break;
                }
                // SEAT
                case (3): {
                    value = this.current[2];
                    break;
                }
                // PRICE
                case (4): {
                    value = (double) this.prices.nextInt();
                    break;
                }
                // BAD MOJO!
                default:

            }
            return (value);
        }
    }

    // -----------------------------------------------------------------
    // FLIGHT IDS
    // -----------------------------------------------------------------

    public Iterable<FlightId> getFlightIds() {
        return (new Iterable<FlightId>() {
            @Override
            public Iterator<FlightId> iterator() {
                return (new Iterator<FlightId>() {
                    private int idx = 0;
                    private final int cnt = SEATSLoader.this.seats_remaining.size();

                    @Override
                    public boolean hasNext() {
                        return (this.idx < this.cnt);
                    }

                    @Override
                    public FlightId next() {
                        return (SEATSLoader.this.seats_remaining.get(this.idx++));
                    }

                    @Override
                    public void remove() {
                        // Not implemented
                    }
                });
            }
        });
    }

    /**
     * @param flight_id
     */
    public boolean addFlightId(FlightId flight_id) {


        this.profile.addFlightId(flight_id);
        this.seats_remaining.put(flight_id, (short) SEATSConstants.FLIGHTS_NUM_SEATS);

        // XXX
        if (this.profile.flight_upcoming_offset == null && this.profile.flight_upcoming_date.compareTo(flight_id.getDepartDateAsTimestamp(this.profile.flight_start_date)) < 0) {
            this.profile.flight_upcoming_offset = (long) (this.seats_remaining.size() - 1);
        }
        return (true);
    }

    /**
     * Return the number of unique flight ids
     *
     * @return
     */
    public long getFlightIdCount() {
        return (this.seats_remaining.size());
    }

    /**
     * Return the index offset of when future flights
     *
     * @return
     */
    public long getFlightIdStartingOffset() {
        return (this.profile.flight_upcoming_offset);
    }

    /**
     * Return the number of seats remaining for a flight
     *
     * @param flight_id
     * @return
     */
    public int getFlightRemainingSeats(FlightId flight_id) {
        return (this.seats_remaining.get(flight_id));
    }

    /**
     * Decrement the number of available seats for a flight and return the total
     * amount remaining
     */
    public int decrementFlightSeat(FlightId flight_id) {
        Short seats = this.seats_remaining.get(flight_id);


        return (this.seats_remaining.put(flight_id, (short) (seats - 1)));
    }

    // ----------------------------------------------------------------
    // DISTANCE METHODS
    // ----------------------------------------------------------------

    public void setDistance(String airport0, String airport1, double distance) {
        short short_distance = (short) Math.round(distance);
        for (String[] a : new String[][]{{airport0, airport1}, {airport1, airport0}}) {
            if (!this.airport_distances.containsKey(a[0])) {
                this.airport_distances.put(a[0], new HashMap<>());
            }
            this.airport_distances.get(a[0]).put(a[1], short_distance);
        }
    }

    public Integer getDistance(String airport0, String airport1) {


        return ((int) this.airport_distances.get(airport0).get(airport1));
    }

    /**
     * For the current depart+arrive airport destinations, calculate the
     * estimated flight time and then add the to the departure time in order to
     * come up with the expected arrival time.
     *
     * @param depart_airport
     * @param arrive_airport
     * @param depart_time
     * @return
     */
    public Timestamp calculateArrivalTime(String depart_airport, String arrive_airport, Timestamp depart_time) {
        Integer distance = this.getDistance(depart_airport, arrive_airport);

        long flight_time = Math.round(distance / SEATSConstants.FLIGHT_TRAVEL_RATE) * 3600000000L;
        // 60 sec * 60 min * 1,000,000
        return (new Timestamp(depart_time.getTime() + flight_time));
    }
}