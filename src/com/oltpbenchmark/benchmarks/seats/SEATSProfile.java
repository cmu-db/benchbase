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

package com.oltpbenchmark.benchmarks.seats;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import com.oltpbenchmark.types.DatabaseType;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.seats.procedures.LoadConfig;
import com.oltpbenchmark.benchmarks.seats.util.CustomerId;
import com.oltpbenchmark.benchmarks.seats.util.FlightId;
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.JSONUtil;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.RandomGenerator;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.StringUtil;

public class SEATSProfile {
    private static final Logger LOG = Logger.getLogger(SEATSProfile.class);

    // ----------------------------------------------------------------
    // PERSISTENT DATA MEMBERS
    // ----------------------------------------------------------------

    /**
     * Data Scale Factor
     */
    protected double scale_factor;
    /**
     * For each airport id, store the last id of the customer that uses this
     * airport as their local airport. The customer ids will be stored as
     * follows in the dbms: <16-bit AirportId><48-bit CustomerId>
     */
    protected final Histogram<Long> airport_max_customer_id = new Histogram<Long>();
    /**
     * The date when flights total data set begins
     */
    protected final Timestamp flight_start_date = new Timestamp(0);
    /**
     * The date for when the flights are considered upcoming and are eligible
     * for reservations
     */
    protected Timestamp flight_upcoming_date;
    /**
     * The number of days in the past that our flight data set includes.
     */
    protected long flight_past_days;
    /**
     * The number of days in the future (from the flight_upcoming_date) that our
     * flight data set includes
     */
    protected long flight_future_days;
    /**
     * The offset of when upcoming flights begin in the seats_remaining list
     */
    protected Long flight_upcoming_offset = null;
    /**
     * The offset of when reservations for upcoming flights begin
     */
    protected Long reservation_upcoming_offset = null;
    /**
     * The number of reservations initially created.
     */
    protected long num_reservations = 0l;

    /**
     * TODO
     **/
    protected final Map<String, Histogram<String>> histograms = new HashMap<String, Histogram<String>>();

    /**
     * Each AirportCode will have a histogram of the number of flights that
     * depart from that airport to all the other airports
     */
    protected final Map<String, Histogram<String>> airport_histograms = new HashMap<String, Histogram<String>>();

    protected final Map<String, Map<String, Long>> code_id_xref = new HashMap<String, Map<String, Long>>();

    // ----------------------------------------------------------------
    // TRANSIENT DATA MEMBERS
    // ----------------------------------------------------------------

    protected final SEATSBenchmark benchmark;

    /**
     * TableName -> TableCatalog
     */
    protected transient final Catalog catalog;

    /**
     * We want to maintain a small cache of FlightIds so that the SEATSClient
     * has something to work with. We obviously don't want to store the entire
     * set here
     */
    protected transient final LinkedList<FlightId> cached_flight_ids = new LinkedList<FlightId>();

    /**
     * Key -> Id Mappings
     */
    protected transient final Map<String, String> code_columns = new HashMap<String, String>();

    /**
     * Foreign Key Mappings Column Name -> Xref Mapper
     */
    protected transient final Map<String, String> fkey_value_xref = new HashMap<String, String>();

    /**
     * Data Directory
     */
    protected transient final File airline_data_dir;

    /**
     * Specialized random number generator
     */
    protected transient final RandomGenerator rng;

    /**
     * Depart Airport Code -> Arrive Airport Code Random number generators based
     * on the flight distributions
     */
    private final Map<String, FlatHistogram<String>> airport_distributions = new HashMap<String, FlatHistogram<String>>();

    // ----------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------

    public SEATSProfile(SEATSBenchmark benchmark, RandomGenerator rng) {
        this.benchmark = benchmark;
        this.catalog = benchmark.getCatalog();
        this.rng = rng;
        this.airline_data_dir = benchmark.getDataDir();
        if (this.airline_data_dir.exists() == false) {
            throw new RuntimeException("Unable to start benchmark. The data directory '" + this.airline_data_dir.getAbsolutePath() + "' does not exist");
        }

        // Tuple Code to Tuple Id Mapping
        for (String xref[] : SEATSConstants.CODE_TO_ID_COLUMNS) {
            assert (xref.length == 3);
            String tableName = xref[0];
            String codeCol = xref[1];
            String idCol = xref[2];

            if (this.code_columns.containsKey(codeCol) == false) {
                this.code_columns.put(codeCol, idCol);
                this.code_id_xref.put(idCol, new HashMap<String, Long>());
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Added %s mapping from Code Column '%s' to Id Column '%s'", tableName, codeCol, idCol));
                }
            }
        } // FOR

        // Foreign Key Code to Ids Mapping
        // In this data structure, the key will be the name of the dependent
        // column and the value will be the name of the foreign key parent
        // column. We then use this in conjunction with the Key->Id mapping
        // to turn a code into a foreign key column id. For example, if the
        // child table AIRPORT has a column with a foreign key reference to
        // COUNTRY.CO_ID, then the data file for AIRPORT will have a value
        // 'USA' in the AP_CO_ID column. We can use mapping to get the id number
        // for 'USA'. Long winded and kind of screwy, but hey what else are
        // you going to do?
        for (Table catalog_tbl : this.catalog.getTables()) {
            for (Column catalog_col : catalog_tbl.getColumns()) {
                Column catalog_fkey_col = catalog_col.getForeignKey();
                if (catalog_fkey_col != null && this.code_id_xref.containsKey(catalog_fkey_col.getName())) {
                    this.fkey_value_xref.put(catalog_col.getName(), catalog_fkey_col.getName());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Added ForeignKey mapping from %s to %s", catalog_col.fullName(), catalog_fkey_col.fullName()));
                    }
                }
            } // FOR
        } // FOR

    }

    // ----------------------------------------------------------------
    // SAVE / LOAD PROFILE
    // ----------------------------------------------------------------

    /**
     * Save the profile information into the database
     */
    protected final void saveProfile(Connection conn) throws SQLException {
        DatabaseType dbType = this.benchmark.getWorkloadConfiguration().getDBType();
        PreparedStatement stmt = null;
        String sql;

        // CONFIG_PROFILE
        Table catalog_tbl = this.catalog.getTable(SEATSConstants.TABLENAME_CONFIG_PROFILE);
        assert (catalog_tbl != null);
        sql = SQLUtil.getInsertSQL(catalog_tbl, dbType);
        stmt = conn.prepareStatement(sql);

        int param_idx = 1;
        stmt.setObject(param_idx++, this.scale_factor); // CFP_SCALE_FACTOR
        stmt.setObject(param_idx++, this.airport_max_customer_id.toJSONString()); // CFP_AIPORT_MAX_CUSTOMER
        stmt.setObject(param_idx++, this.flight_start_date); // CFP_FLIGHT_START
        stmt.setObject(param_idx++, this.flight_upcoming_date); // CFP_FLIGHT_UPCOMING
        stmt.setObject(param_idx++, this.flight_past_days); // CFP_FLIGHT_PAST_DAYS
        stmt.setObject(param_idx++, this.flight_future_days); // CFP_FLIGHT_FUTURE_DAYS
        stmt.setObject(param_idx++, this.flight_upcoming_offset); // CFP_FLIGHT_OFFSET
        stmt.setObject(param_idx++, this.reservation_upcoming_offset); // CFP_RESERVATION_OFFSET
        stmt.setObject(param_idx++, this.num_reservations); // CFP_NUM_RESERVATIONS
        stmt.setObject(param_idx++, JSONUtil.toJSONString(this.code_id_xref)); // CFP_CODE_ID_XREF
        int result = stmt.executeUpdate();
        conn.commit();
        stmt.close();
        assert (result == 1);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Saved profile information into " + catalog_tbl.getName());
        }

        // CONFIG_HISTOGRAMS
        catalog_tbl = this.catalog.getTable(SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS);
        sql = SQLUtil.getInsertSQL(catalog_tbl, dbType);
        stmt = conn.prepareStatement(sql);
        for (Entry<String, Histogram<String>> e : this.airport_histograms.entrySet()) {
            param_idx = 1;
            stmt.setObject(param_idx++, e.getKey()); // CFH_NAME
            stmt.setObject(param_idx++, e.getValue().toJSONString()); // CFH_DATA
            stmt.setObject(param_idx++, 1); // CFH_IS_AIRPORT
            result = stmt.executeUpdate();
            assert (result == 1);
        } // FOR
        if (LOG.isDebugEnabled()) {
            LOG.debug("Saved airport histogram information into " + catalog_tbl.getName());
        }

        for (Entry<String, Histogram<String>> e : this.histograms.entrySet()) {
            param_idx = 1;
            stmt.setObject(param_idx++, e.getKey()); // CFH_NAME
            stmt.setObject(param_idx++, e.getValue().toJSONString()); // CFH_DATA
            stmt.setObject(param_idx++, 0); // CFH_IS_AIRPORT
            result = stmt.executeUpdate();
            assert (result == 1);
        } // FOR
        conn.commit();
        stmt.close();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Saved benchmark histogram information into " + catalog_tbl.getName());
        }

        return;
    }

    protected static void clearCachedProfile() {
        cachedProfile = null;
    }

    private SEATSProfile copy(SEATSProfile other) {
        this.scale_factor = other.scale_factor;
        this.airport_max_customer_id.putHistogram(other.airport_max_customer_id);
        this.flight_start_date.setTime(other.flight_start_date.getTime());
        this.flight_upcoming_date = other.flight_upcoming_date;
        this.flight_past_days = other.flight_past_days;
        this.flight_future_days = other.flight_future_days;
        this.flight_upcoming_offset = other.flight_upcoming_offset;
        this.reservation_upcoming_offset = other.reservation_upcoming_offset;
        this.num_reservations = other.num_reservations;
        this.code_id_xref.putAll(other.code_id_xref);
        this.cached_flight_ids.addAll(other.cached_flight_ids);
        this.airport_histograms.putAll(other.airport_histograms);
        this.histograms.putAll(other.histograms);
        return (this);
    }

    /**
     * Load the profile information stored in the database
     */
    private static SEATSProfile cachedProfile;

    protected final void loadProfile(SEATSWorker worker) throws SQLException {
        synchronized (SEATSProfile.class) {
            // Check whether we have a cached Profile we can copy from
            if (cachedProfile != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Using cached SEATSProfile");
                }
                this.copy(cachedProfile);
                return;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Loading SEATSProfile for the first time");
            }

            // Otherwise we have to go fetch everything again
            LoadConfig proc = worker.getProcedure(LoadConfig.class);
            ResultSet results[] = proc.run(worker.getConnection());
            int result_idx = 0;

            // CONFIG_PROFILE
            this.loadConfigProfile(results[result_idx++]);

            // CONFIG_HISTOGRAMS
            this.loadConfigHistograms(results[result_idx++]);

            // CODE XREFS
            for (int i = 0; i < SEATSConstants.CODE_TO_ID_COLUMNS.length; i++) {
                String codeCol = SEATSConstants.CODE_TO_ID_COLUMNS[i][1];
                String idCol = SEATSConstants.CODE_TO_ID_COLUMNS[i][2];
                this.loadCodeXref(results[result_idx++], codeCol, idCol);
            } // FOR

            // CACHED FLIGHT IDS
            this.loadCachedFlights(results[result_idx++]);

            for (ResultSet rs : results) {
                rs.close();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Loaded profile:\n" + this.toString());
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("Airport Max Customer Id:\n" + this.airport_max_customer_id);
            }

            cachedProfile = new SEATSProfile(this.benchmark, this.rng).copy(this);
        } // SYNCH
    }

    private final void loadConfigProfile(ResultSet vt) throws SQLException {
        boolean adv = vt.next();
        assert (adv);
        int col = 1;
        this.scale_factor = vt.getDouble(col++);
        JSONUtil.fromJSONString(this.airport_max_customer_id, vt.getString(col++));
        this.flight_start_date.setTime(vt.getTimestamp(col++).getTime());
        this.flight_upcoming_date = vt.getTimestamp(col++);
        this.flight_past_days = vt.getLong(col++);
        this.flight_future_days = vt.getLong(col++);
        this.flight_upcoming_offset = vt.getLong(col++);
        this.reservation_upcoming_offset = vt.getLong(col++);
        this.num_reservations = vt.getLong(col++);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Loaded %s data", SEATSConstants.TABLENAME_CONFIG_PROFILE));
        }
    }

    private final void loadConfigHistograms(ResultSet vt) throws SQLException {
        while (vt.next()) {
            int col = 1;
            String name = vt.getString(col++);
            Histogram<String> h = JSONUtil.fromJSONString(new Histogram<String>(), vt.getString(col++));
            boolean is_airline = (vt.getLong(col++) == 1);

            if (is_airline) {
                this.airport_histograms.put(name, h);
                if (LOG.isTraceEnabled()) {
                    LOG.trace(String.format("Loaded %d records for %s airport histogram", h.getValueCount(), name));
                }
            } else {
                this.histograms.put(name, h);
                if (LOG.isTraceEnabled()) {
                    LOG.trace(String.format("Loaded %d records for %s histogram", h.getValueCount(), name));
                }
            }
        } // WHILE
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Loaded %s data", SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS));
        }
    }

    private final void loadCodeXref(ResultSet vt, String codeCol, String idCol) throws SQLException {
        Map<String, Long> m = this.code_id_xref.get(idCol);
        while (vt.next()) {
            int col = 1;
            long id = vt.getLong(col++);
            String code = vt.getString(col++);
            m.put(code, id);
        } // WHILE
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Loaded %d xrefs for %s -> %s", m.size(), codeCol, idCol));
        }
    }

    private final void loadCachedFlights(ResultSet vt) throws SQLException {
        int limit = 1;
        while (vt.next() && limit++ < SEATSConstants.CACHE_LIMIT_FLIGHT_IDS) {
            int col = 1;
            long f_id = vt.getLong(col++);
            FlightId flight_id = new FlightId(f_id);
            this.cached_flight_ids.add(flight_id);
        } // WHILE
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Loaded %d cached FlightIds", this.cached_flight_ids.size()));
        }
    }

    // ----------------------------------------------------------------
    // DATA ACCESS METHODS
    // ----------------------------------------------------------------

    public File getSEATSDataDir() {
        return this.airline_data_dir;
    }

    private Map<String, Long> getCodeXref(String col_name) {
        Map<String, Long> m = this.code_id_xref.get(col_name);
        assert (m != null) : "Invalid code xref mapping column '" + col_name + "'";
        assert (m.isEmpty() == false) : "Empty code xref mapping for column '" + col_name + "'\n" + StringUtil.formatMaps(this.code_id_xref);
        return (m);
    }

    /**
     * The offset of when upcoming reservation ids begin
     *
     * @return
     */
    public Long getReservationUpcomingOffset() {
        return (this.reservation_upcoming_offset);
    }

    /**
     * Set the number of upcoming reservation offset
     *
     * @param numReservations
     */
    public void setReservationUpcomingOffset(long offset) {
        this.reservation_upcoming_offset = offset;
    }

    // -----------------------------------------------------------------
    // FLIGHTS
    // -----------------------------------------------------------------

    /**
     * Add a new FlightId for this benchmark instance This method will decide
     * whether to store the id or not in its cache
     *
     * @return True if the FlightId was added to the cache
     */
    public boolean addFlightId(FlightId flight_id) {
        boolean added = false;
        synchronized (this.cached_flight_ids) {
            // If we have room, shove it right in
            // We'll throw it in the back because we know it hasn't been used
            // yet
            if (this.cached_flight_ids.size() < SEATSConstants.CACHE_LIMIT_FLIGHT_IDS) {
                this.cached_flight_ids.addLast(flight_id);
                added = true;

                // Otherwise, we can will randomly decide whether to pop one out
            } else if (this.rng.nextBoolean()) {
                this.cached_flight_ids.pop();
                this.cached_flight_ids.addLast(flight_id);
                added = true;
            }
        } // SYNCH
        return (added);
    }

    public long getFlightIdCount() {
        return (this.cached_flight_ids.size());
    }

    // ----------------------------------------------------------------
    // HISTOGRAM METHODS
    // ----------------------------------------------------------------

    /**
     * Return the histogram for the given name
     *
     * @param name
     * @return
     */
    public Histogram<String> getHistogram(String name) {
        Histogram<String> h = this.histograms.get(name);
        assert (h != null) : "Invalid histogram '" + name + "'";
        return (h);
    }

    /**
     * @param airport_code
     * @return
     */
    public Histogram<String> getFightsPerAirportHistogram(String airport_code) {
        return (this.airport_histograms.get(airport_code));
    }

    /**
     * Returns the number of histograms that we have loaded Does not include the
     * airport_histograms
     *
     * @return
     */
    public int getHistogramCount() {
        return (this.histograms.size());
    }

    // ----------------------------------------------------------------
    // RANDOM GENERATION METHODS
    // ----------------------------------------------------------------

    /**
     * Return a random airport id
     *
     * @return
     */
    public long getRandomAirportId() {
        return (this.rng.number(1, this.getAirportCount()));
    }

    public long getRandomOtherAirport(long airport_id) {
        String code = this.getAirportCode(airport_id);
        FlatHistogram<String> f = this.airport_distributions.get(code);
        if (f == null) {
            synchronized (this.airport_distributions) {
                f = this.airport_distributions.get(code);
                if (f == null) {
                    Histogram<String> h = this.airport_histograms.get(code);
                    assert (h != null);
                    f = new FlatHistogram<String>(this.rng, h);
                    this.airport_distributions.put(code, f);
                }
            } // SYCH
        }
        assert (f != null);
        String other = f.nextValue();
        return this.getAirportId(other);
    }

    /**
     * Return a random customer id based at the given airport_id
     *
     * @param airport_id
     * @return
     */
    public CustomerId getRandomCustomerId(Long airport_id) {
        Integer cnt = this.getCustomerIdCount(airport_id);
        if (cnt != null) {
            int base_id = this.rng.nextInt(cnt.intValue());
            return (new CustomerId(base_id, airport_id));
        }
        return (null);
    }

    /**
     * Return a random customer id based out of any airport
     *
     * @return
     */
    public CustomerId getRandomCustomerId() {
        int num_airports = this.airport_max_customer_id.getValueCount();
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Selecting a random airport with customers [numAirports=%d]", num_airports));
        }
        CustomerId c_id = null;
        while (c_id == null) {
            Long airport_id = (long) this.rng.number(1, num_airports);
            c_id = this.getRandomCustomerId(airport_id);
        } // WHILE
        return (c_id);
    }

    /**
     * Return a random date in the future (after the start of upcoming flights)
     *
     * @return
     */
    public Timestamp getRandomUpcomingDate() {
        Timestamp upcoming_start_date = this.flight_upcoming_date;
        int offset = this.rng.nextInt((int) this.getFlightFutureDays());
        return (new Timestamp(upcoming_start_date.getTime() + (offset * SEATSConstants.MILLISECONDS_PER_DAY)));
    }

    /**
     * Return a random FlightId from our set of cached ids
     *
     * @return
     */
    public FlightId getRandomFlightId() {
        assert (this.cached_flight_ids.isEmpty() == false);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Attempting to get a random FlightId");
        }
        int idx = this.rng.nextInt(this.cached_flight_ids.size());
        FlightId flight_id = this.cached_flight_ids.get(idx);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Got random " + flight_id);
        }
        return (flight_id);
    }
    // ----------------------------------------------------------------
    // AIRLINE METHODS
    // ----------------------------------------------------------------

    public Collection<Long> getAirlineIds() {
        Map<String, Long> m = this.getCodeXref("AL_ID");
        return (m.values());
    }

    public Collection<String> getAirlineCodes() {
        Map<String, Long> m = this.getCodeXref("AL_ID");
        return (m.keySet());
    }

    public Long getAirlineId(String airline_code) {
        Map<String, Long> m = this.getCodeXref("AL_ID");
        return (m.get(airline_code));
    }

    public int incrementAirportCustomerCount(long airport_id) {
        int next_id = this.airport_max_customer_id.get(airport_id, 0);
        this.airport_max_customer_id.put(airport_id);
        return (next_id);
    }

    public Integer getCustomerIdCount(Long airport_id) {
        return (this.airport_max_customer_id.get(airport_id));
    }

    public long getCustomerIdCount() {
        return (this.airport_max_customer_id.getSampleCount());
    }

    // ----------------------------------------------------------------
    // AIRPORT METHODS
    // ----------------------------------------------------------------

    /**
     * Return all the airport ids that we know about
     *
     * @return
     */
    public Collection<Long> getAirportIds() {
        Map<String, Long> m = this.getCodeXref("AP_ID");
        return (m.values());
    }

    public Long getAirportId(String airport_code) {
        Map<String, Long> m = this.getCodeXref("AP_ID");
        return (m.get(airport_code));
    }

    public String getAirportCode(long airport_id) {
        Map<String, Long> m = this.getCodeXref("AP_ID");
        for (Entry<String, Long> e : m.entrySet()) {
            if (e.getValue() == airport_id) {
                return (e.getKey());
            }
        }
        return (null);
    }

    public Collection<String> getAirportCodes() {
        return (this.getCodeXref("AP_ID").keySet());
    }

    /**
     * Return the number of airports that are part of this profile
     *
     * @return
     */
    public int getAirportCount() {
        return (this.getAirportCodes().size());
    }

    public Histogram<String> getAirportCustomerHistogram() {
        Histogram<String> h = new Histogram<String>();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Generating Airport-CustomerCount histogram [numAirports=" + this.getAirportCount() + "]");
        }
        for (Long airport_id : this.airport_max_customer_id.values()) {
            String airport_code = this.getAirportCode(airport_id);
            int count = this.airport_max_customer_id.get(airport_id);
            h.put(airport_code, count);
        } // FOR
        return (h);
    }

    public Collection<String> getAirportsWithFlights() {
        return this.airport_histograms.keySet();
    }

    public boolean hasFlights(String airport_code) {
        Histogram<String> h = this.getFightsPerAirportHistogram(airport_code);
        if (h != null) {
            return (h.getSampleCount() > 0);
        }
        return (false);
    }

    // -----------------------------------------------------------------
    // FLIGHT DATES
    // -----------------------------------------------------------------

    /**
     * The date in which the flight data set begins
     *
     * @return
     */
    public Timestamp getFlightStartDate() {
        return this.flight_start_date;
    }

    /**
     * @param start_date
     */
    public void setFlightStartDate(Timestamp start_date) {
        this.flight_start_date.setTime(start_date.getTime());
    }

    /**
     * The date in which the flight data set begins
     *
     * @return
     */
    public Timestamp getFlightUpcomingDate() {
        return (this.flight_upcoming_date);
    }

    /**
     * @param startDate
     */
    public void setFlightUpcomingDate(Timestamp upcoming_date) {
        this.flight_upcoming_date = upcoming_date;
    }

    /**
     * The date in which upcoming flights begin
     *
     * @return
     */
    public long getFlightPastDays() {
        return (this.flight_past_days);
    }

    /**
     * @param flight_start_date
     */
    public void setFlightPastDays(long flight_past_days) {
        this.flight_past_days = flight_past_days;
    }

    /**
     * The date in which upcoming flights begin
     *
     * @return
     */
    public long getFlightFutureDays() {
        return (this.flight_future_days);
    }

    /**
     * @param flight_start_date
     */
    public void setFlightFutureDays(long flight_future_days) {
        this.flight_future_days = flight_future_days;
    }

    public long getNextReservationId(int id) {
        // Offset it by the client id so that we can ensure it's unique
        return (id | this.num_reservations++ << 48);
    }

    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Scale Factor", this.scale_factor);
        m.put("Data Directory", this.airline_data_dir);
        m.put("# of Reservations", this.num_reservations);
        m.put("Flight Start Date", this.flight_start_date);
        m.put("Flight Upcoming Date", this.flight_upcoming_date);
        m.put("Flight Past Days", this.flight_past_days);
        m.put("Flight Future Days", this.flight_future_days);
        m.put("Flight Upcoming Offset", this.flight_upcoming_offset);
        m.put("Reservation Upcoming Offset", this.reservation_upcoming_offset);
        return (StringUtil.formatMaps(m));
    }

}
