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
/* This file is part of VoltDB. 
 * Copyright (C) 2009 Vertica Systems Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR 
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.                       
 */

package com.oltpbenchmark.benchmarks.seats;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.api.Procedure.UserAbortException;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants.ErrorType;
import com.oltpbenchmark.benchmarks.seats.procedures.DeleteReservation;
import com.oltpbenchmark.benchmarks.seats.procedures.FindFlights;
import com.oltpbenchmark.benchmarks.seats.procedures.FindOpenSeats;
import com.oltpbenchmark.benchmarks.seats.procedures.NewReservation;
import com.oltpbenchmark.benchmarks.seats.procedures.UpdateCustomer;
import com.oltpbenchmark.benchmarks.seats.procedures.UpdateReservation;
import com.oltpbenchmark.benchmarks.seats.util.CustomerId;
import com.oltpbenchmark.benchmarks.seats.util.FlightId;
import com.oltpbenchmark.benchmarks.tatp.TATPWorker.Transaction;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.Pair;
import com.oltpbenchmark.util.RandomDistribution;
import com.oltpbenchmark.util.RandomGenerator;
import com.oltpbenchmark.util.StringUtil;

public class SEATSWorker extends Worker {
    private static final Logger LOG = Logger.getLogger(SEATSWorker.class);

    /**
     * Airline Benchmark Transactions
     */
    public static enum Transaction {
        DELETE_RESERVATION          (DeleteReservation.class,   SEATSConstants.FREQUENCY_DELETE_RESERVATION),
        FIND_FLIGHTS                (FindFlights.class,         SEATSConstants.FREQUENCY_FIND_FLIGHTS),
        FIND_OPEN_SEATS             (FindOpenSeats.class,       SEATSConstants.FREQUENCY_FIND_OPEN_SEATS),
        NEW_RESERVATION             (NewReservation.class,      SEATSConstants.FREQUENCY_NEW_RESERVATION),
        UPDATE_CUSTOMER             (UpdateCustomer.class,      SEATSConstants.FREQUENCY_UPDATE_CUSTOMER),
        UPDATE_RESERVATION          (UpdateReservation.class,   SEATSConstants.FREQUENCY_UPDATE_RESERVATION);
        
        private Transaction(Class<? extends Procedure> proc_class, int weight) {
            this.proc_class = proc_class;
            this.execName = proc_class.getSimpleName();
            this.default_weight = weight;
            this.displayName = StringUtil.title(this.name().replace("_", " "));
        }

        public final Class<? extends Procedure> proc_class;
        public final int default_weight;
        public final String displayName;
        public final String execName;
        
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<String, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name().toLowerCase().intern(), vt);
            }
        }
        
        public static Transaction get(Integer idx) {
            assert(idx >= 0);
            return (Transaction.idx_lookup.get(idx));
        }

        public static Transaction get(String name) {
            return (Transaction.name_lookup.get(name.toLowerCase().intern()));
        }
        public int getDefaultWeight() {
            return (this.default_weight);
        }
        public String getDisplayName() {
            return (this.displayName);
        }
        public String getExecName() {
            return (this.execName);
        }
    }
    
    // -----------------------------------------------------------------
    // SEPARATE CALLBACK PROCESSING THREAD
    // -----------------------------------------------------------------
    
    protected abstract class AbstractCallback<T> {
        final Transaction txn;
        final T element;
        public AbstractCallback(Transaction txn, T t) {
            this.txn = txn;
            this.element = t;
        }
        public final void clientCallback(ResultSet[] clientResponse) {
            callbackQueue.add(new Pair<AbstractCallback<?>, ResultSet[]>(this, clientResponse));
        }
        public abstract void clientCallbackImpl(ResultSet[] clientResponse);
    }
    
    private static Thread callbackThread;
    private static final LinkedBlockingQueue<Pair<AbstractCallback<?>, ResultSet[]>> callbackQueue = new LinkedBlockingQueue<Pair<AbstractCallback<?>,ResultSet[]>>();

    
    // -----------------------------------------------------------------
    // RESERVED SEAT BITMAPS
    // -----------------------------------------------------------------
    
    public enum CacheType {
        PENDING_INSERTS     (SEATSConstants.CACHE_LIMIT_PENDING_INSERTS),
        PENDING_UPDATES     (SEATSConstants.CACHE_LIMIT_PENDING_UPDATES),
        PENDING_DELETES     (SEATSConstants.CACHE_LIMIT_PENDING_DELETES),
        ;
        
        private CacheType(int limit) {
            this.limit = limit;
            this.lock = new ReentrantLock();
        }
        
        private final int limit;
        private final ReentrantLock lock;
    }
    
    protected final Map<CacheType, LinkedList<Reservation>> CACHE_RESERVATIONS = new HashMap<SEATSWorker.CacheType, LinkedList<Reservation>>();
    {
        for (CacheType ctype : CacheType.values()) {
            CACHE_RESERVATIONS.put(ctype, new LinkedList<Reservation>());
        } // FOR
    } // STATIC 
    
    
    protected static final ConcurrentHashMap<CustomerId, Set<FlightId>> CACHE_CUSTOMER_BOOKED_FLIGHTS = new ConcurrentHashMap<CustomerId, Set<FlightId>>();
    protected static final Map<FlightId, BitSet> CACHE_BOOKED_SEATS = new HashMap<FlightId, BitSet>();

    
    private static final BitSet FULL_FLIGHT_BITSET = new BitSet(SEATSConstants.NUM_SEATS_PER_FLIGHT);
    static {
        for (int i = 0; i < SEATSConstants.NUM_SEATS_PER_FLIGHT; i++)
            FULL_FLIGHT_BITSET.set(i);
    } // STATIC
    
    protected static BitSet getSeatsBitSet(FlightId flight_id) {
        BitSet seats = CACHE_BOOKED_SEATS.get(flight_id);
        if (seats == null) {
            synchronized (CACHE_BOOKED_SEATS) {
                seats = CACHE_BOOKED_SEATS.get(flight_id);
                if (seats == null) {
                    seats = new BitSet(SEATSConstants.NUM_SEATS_PER_FLIGHT);
                    CACHE_BOOKED_SEATS.put(flight_id, seats);
                }
            } // SYNCH
        }
        return (seats);
    }
    
    /**
     * Returns true if the given BitSet for a Flight has all of its seats reserved 
     * @param seats
     * @return
     */
    protected static boolean isFlightFull(BitSet seats) {
        assert(FULL_FLIGHT_BITSET.size() == seats.size());
        return FULL_FLIGHT_BITSET.equals(seats);
    }
    
    /**
     * Returns true if the given Customer already has a reservation booked on the target Flight
     * @param customer_id
     * @param flight_id
     * @return
     */
    protected boolean isCustomerBookedOnFlight(CustomerId customer_id, FlightId flight_id) {
        Set<FlightId> flights = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        return (flights != null && flights.contains(flight_id));
    }

    /**
     * Returns the set of Customers that are waiting to be added the given Flight
     * @param flight_id
     * @return
     */
    protected Set<CustomerId> getPendingCustomers(FlightId flight_id) {
        Set<CustomerId> customers = new HashSet<CustomerId>();
        CacheType.PENDING_INSERTS.lock.lock();
        try {
            for (Reservation r : CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS)) {
                if (r.flight_id.equals(flight_id)) customers.add(r.customer_id);
            } // FOR
        } finally {
            CacheType.PENDING_INSERTS.lock.unlock();
        } // SYNCH
        return (customers);
    }
    
    /**
     * Returns true if the given Customer is pending to be booked on the given Flight
     * @param customer_id
     * @param flight_id
     * @return
     */
    protected boolean isCustomerPendingOnFlight(CustomerId customer_id, FlightId flight_id) {
        CacheType.PENDING_INSERTS.lock.lock();
        try {
            for (Reservation r : CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS)) {
                if (r.flight_id.equals(flight_id) && r.customer_id.equals(customer_id)) {
                    return (true);
                }
            } // FOR
        } finally {
            CacheType.PENDING_INSERTS.lock.unlock();
        } // SYNCH
        return (false);
    }
    
    protected Set<FlightId> getCustomerBookedFlights(CustomerId customer_id) {
        Set<FlightId> f_ids = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        if (f_ids == null) {
            synchronized (CACHE_CUSTOMER_BOOKED_FLIGHTS) {
                f_ids = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
                if (f_ids == null) {
                    f_ids = new HashSet<FlightId>();
                    CACHE_CUSTOMER_BOOKED_FLIGHTS.put(customer_id, f_ids);
                }
            } // SYNCH
        }
        return (f_ids);
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (CacheType ctype : CACHE_RESERVATIONS.keySet()) {
            m.put(ctype.name(), CACHE_RESERVATIONS.get(ctype));
        } // FOR
        m.put("CACHE_CUSTOMER_BOOKED_FLIGHTS", CACHE_CUSTOMER_BOOKED_FLIGHTS); 
        m.put("CACHE_BOOKED_SEATS", CACHE_BOOKED_SEATS); 
        
        return StringUtil.formatMaps(m);
    }
    
    // -----------------------------------------------------------------
    // ADDITIONAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    private final SEATSProfile profile;
    private final RandomGenerator rng;
    private final AtomicBoolean first = new AtomicBoolean(true);
    private final RandomDistribution.FlatHistogram<Transaction> xacts;
    
    /**
     * When a customer looks for an open seat, they will then attempt to book that seat in
     * a new reservation. Some of them will want to change their seats. This data structure
     * represents a customer that is queued to change their seat. 
     */
    protected static class Reservation {
        public final long id;
        public final FlightId flight_id;
        public final CustomerId customer_id;
        public final int seatnum;
        
        public Reservation(long id, FlightId flight_id, CustomerId customer_id, int seatnum) {
            this.id = id;
            this.flight_id = flight_id;
            this.customer_id = customer_id;
            this.seatnum = seatnum;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Reservation) {
                Reservation r = (Reservation)obj;
                // Ignore id!
                return (this.seatnum == r.seatnum &&
                        this.flight_id.equals(r.flight_id) &&
                        this.customer_id.equals(r.customer_id));
                        
            }
            return (false);
        }
        
        @Override
        public String toString() {
            return String.format("{Id:%d / %s / %s / SeatNum:%d}",
                                 this.id, this.flight_id, this.customer_id, this.seatnum);
        }
    } // END CLASS

    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    public SEATSWorker(int id, SEATSBenchmark benchmark) {
        super(id, benchmark);

        // Initialize Default Weights
        final Histogram<Transaction> weights = new Histogram<Transaction>();
        for (Transaction t : Transaction.values()) {
            weights.put(t, t.getDefaultWeight());
        } // FOR

        this.profile = new SEATSProfile(benchmark, benchmark.getRandomGenerator()); 
        try {
            this.profile.loadProfile(this.conn);
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to load profile from database", ex);
        }
        if (LOG.isTraceEnabled()) LOG.trace("Airport Max Customer Id:\n" + this.profile.airport_max_customer_id);
        
        // Make sure we have the information we need in the BenchmarkProfile
        String error_msg = null;
        if (this.profile.getFlightIdCount() == 0) {
            error_msg = "The benchmark profile does not have any flight ids.";
        } else if (this.profile.getCustomerIdCount() == 0) {
            error_msg = "The benchmark profile does not have any customer ids.";
        } else if (this.profile.getFlightStartDate() == null) {
            error_msg = "The benchmark profile does not have a valid flight start date.";
        }
        if (error_msg != null) throw new RuntimeException(error_msg);
        
        // Create xact lookup array
        this.rng = benchmark.getRandomGenerator(); // TODO: Sync with the base class rng
        this.xacts = new RandomDistribution.FlatHistogram<Transaction>(rng, weights);
        assert(weights.getSampleCount() == 100) : "The total weight for the transactions is " + this.xacts.getSampleCount() + ". It needs to be 100";
        if (LOG.isDebugEnabled()) LOG.debug("Transaction Execution Distribution:\n" + weights);
    }

    @Override
    protected TransactionType doWork(boolean measure, Phase phase) {
        TransactionType next = transactionTypes.getType(phase.chooseTransaction());
        this.executeWork(next);
        return (next);
    }

    @Override
    protected void executeWork(TransactionType txnType) {
        Transaction t = Transaction.get(txnType.getName());
        assert(t != null) : "Unexpected " + txnType;
        
        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        assert(proc != null) : String.format("Failed to get Procedure handle for %s.%s",
                                             this.benchmarkModule.getBenchmarkName(), txnType);
        if (LOG.isDebugEnabled()) LOG.debug("Executing " + proc);
        try {
            try {
//                t.invoke(this.conn, proc, subscriberSize);
                this.conn.commit();
            } catch (UserAbortException ex) {
                if (LOG.isDebugEnabled()) LOG.debug(proc + " Aborted", ex);
                this.conn.rollback();
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Unexpected error when executing " + proc, ex);
        }
        
        
    }
    
    
//    protected boolean runOnce() throws IOException {
//        if (this.first.compareAndSet(true, false)) {
//            // Fire off a FindOpenSeats so that we can prime ourselves
//            try {
//                boolean ret = this.executeFindOpenSeats(Transaction.FIND_OPEN_SEATS);
//                assert(ret);
//            } catch (IOException ex) {
//                throw new RuntimeException(ex);
//            }
//        }
//        
//        int tries = 10;
//        boolean ret = false;
//        while (tries-- > 0 && ret == false) {
//            Transaction txn = this.xacts.nextValue();
//            if (LOG.isDebugEnabled()) LOG.debug("Attempting to execute " + txn);
//            switch (txn) {
//                case DELETE_RESERVATION: {
//                    ret = this.executeDeleteReservation(txn);
//                    break;
//                }
//                case FIND_FLIGHTS: {
//                    ret = this.executeFindFlights(txn);
//                    break;
//                }
//                case FIND_OPEN_SEATS: {
//                    ret = this.executeFindOpenSeats(txn);
//                    break;
//                }
//                case NEW_RESERVATION: {
//                    ret = this.executeNewReservation(txn);
//                    break;
//                }
//                case UPDATE_CUSTOMER: {
//                    ret = this.executeUpdateCustomer(txn);
//                    break;
//                }
//                case UPDATE_RESERVATION: {
//                    ret = this.executeUpdateReservation(txn);
//                    break;
//                }
//                default:
//                    assert(false) : "Unexpected transaction: " + txn; 
//            } // SWITCH
//            if (ret && LOG.isDebugEnabled()) LOG.debug("Executed a new invocation of " + txn);
//        }
//        if (tries == 0) LOG.warn("I have nothing to do!");
//        return (tries > 0);
//    }
//    
//    @Override
//    public void tick(int counter) {
//        super.tick(counter);
//        for (CacheType ctype : CACHE_RESERVATIONS.keySet()) {
//            ctype.lock.lock();
//            try {
//                List<Reservation> cache = CACHE_RESERVATIONS.get(ctype);
//                int before = cache.size();
//                if (before > ctype.limit) {
//                    Collections.shuffle(cache, rng);
//                    while (cache.size() > ctype.limit) {
//                        cache.remove(0);
//                    } // WHILE
//                    if (LOG.isDebugEnabled()) LOG.debug(String.format("Pruned records from cache [newSize=%d, origSize=%d]",
//                                               cache.size(), before));
//                } // IF
//            } finally {
//                ctype.lock.unlock();
//            } // SYNCH
//        } // FOR
//        
//        if (this.getId() == 0) {
//            LOG.info("NewReservation Errors:\n" + newReservationErrors);
//            newReservationErrors.clear();
//        }
//    }
    
    /**
     * Take an existing Reservation that we know is legit and randomly decide to 
     * either queue it for a later update or delete transaction 
     * @param r
     */
    protected void requeueReservation(Reservation r) {
        int val = rng.nextInt(100);
        
        // Queue this motha trucka up for a deletin'
        if (val < SEATSConstants.PROB_DELETE_NEW_RESERVATION) {
            CacheType.PENDING_DELETES.lock.lock();
            try {
                CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES).add(r);
            } finally {
                CacheType.PENDING_DELETES.lock.unlock();
            } // SYNCH
        }
        // Or queue it for an update
        else if (val < SEATSConstants.PROB_UPDATE_NEW_RESERVATION + SEATSConstants.PROB_DELETE_NEW_RESERVATION) {
            CacheType.PENDING_UPDATES.lock.lock();
            try {
                CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES).add(r);
            } finally {
                CacheType.PENDING_UPDATES.lock.unlock();
            } // SYNCH
        }
    }
    
    
    protected static class CallbackProcessor implements Runnable {
        
        @Override
        public void run() {
            Pair<AbstractCallback<?>, ResultSet[]> p = null;
            while (true) {
                try {
                    p = callbackQueue.take();
                } catch (InterruptedException ex) {
                    break;
                }
                if (LOG.isTraceEnabled()) LOG.trace("CallbackProcessor -> " + p.getFirst().getClass().getSimpleName());
                p.getFirst().clientCallbackImpl(p.getSecond());
            } // WHILE
        }
    }
    
    
    // -----------------------------------------------------------------
    // DeleteReservation
    // -----------------------------------------------------------------
    
    class DeleteReservationCallback extends AbstractCallback<Reservation> {
        public DeleteReservationCallback(Reservation r) {
            super(Transaction.DELETE_RESERVATION, r);
        }
        @Override
        public void clientCallbackImpl(ResultSet clientResponse[]) {
//            if (clientResponse.getStatus() == Hstore.Status.OK) {
//                // We can remove this from our set of full flights because know that there is now a free seat
//                BitSet seats = SEATSWorker.getSeatsBitSet(element.flight_id);
//                seats.set(element.seatnum, false);
//                
//                // And then put it up for a pending insert
//                if (rng.nextInt(100) < SEATSConstants.PROB_REQUEUE_DELETED_RESERVATION) {
//                    CacheType.PENDING_INSERTS.lock.lock();
//                    try {
//                        CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS).add(element);
//                    } finally {
//                        CacheType.PENDING_INSERTS.lock.unlock();
//                    } // SYNCH
//                }
//                
//            } else if (LOG.isDebugEnabled()) {
//                LOG.info("DeleteReservation " + clientResponse.getStatus() + ": " + clientResponse.getStatusString(), clientResponse.getException());
//                LOG.info("BUSTED ID: " + element.flight_id + " / " + element.flight_id.encode());
//            }
        }
    }

    private boolean executeDeleteReservation(Transaction txn) throws IOException {
        // Pull off the first cached reservation and drop it on the cluster...
        Reservation r = CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES).poll();
        if (r == null) {
            return (false);
        }
        int rand = rng.number(1, 100);
        
        Object params[] = new Object[]{
            r.flight_id.encode(),       // [0] f_id
            null,       // [1] c_id
            "",                         // [2] c_id_str
            "",                         // [3] ff_c_id_str
            null,       // [4] ff_al_id
        };
        
        // Delete with the Customer's id as a string 
        if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR) {
            params[2] = Long.toString(r.customer_id.encode());
        }
        // Delete using their FrequentFlyer information
        else if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR + SEATSConstants.PROB_DELETE_WITH_FREQUENTFLYER_ID_STR) {
            params[3] = Long.toString(r.customer_id.encode());
            params[4] = r.flight_id.getSEATSId();
        }
        // Delete using their Customer id
        else {
            params[1] = r.customer_id.encode();
        }
        
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + txn.getExecName());
//        this.getClientHandle().callProcedure(new DeleteReservationCallback(r),
//                                             txn.getExecName(),
//                                             params);
        return (true);
    }
    
    // ----------------------------------------------------------------
    // FindFlights
    // ----------------------------------------------------------------
    
    class FindFlightsCallback { // implements ProcedureCallback {
//        @Override
        public void clientCallback(ResultSet[] clientResponse) {
//            VoltTable[] results = clientResponse.getResults();
//            if (results.length > 1) {
//                // Convert the data into a FlightIds that other transactions can use
//                int ctr = 0;
//                while (results[0].advanceRow()) {
//                    FlightId flight_id = new FlightId(results[0].getLong(0));
//                    assert(flight_id != null);
//                    boolean added = profile.addFlightId(flight_id);
//                    if (added) ctr++;
//                } // WHILE
//                if (LOG.isDebugEnabled()) LOG.debug(String.format("Added %d out of %d FlightIds to local cache",
//                                           ctr, results[0].getRowCount()));
//            }
        }
    }

    /**
     * Execute one of the FindFlight transactions
     * @param txn
     * @throws IOException
     */
    private boolean executeFindFlights(Transaction txn) throws IOException {
        long depart_airport_id;
        long arrive_airport_id;
        Date start_date;
        Date stop_date;
        
        // Select two random airport ids
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_RANDOM_AIRPORTS) {
            // Does it matter whether the one airport actually flies to the other one?
            depart_airport_id = this.profile.getRandomAirportId();
            arrive_airport_id = this.profile.getRandomOtherAirport(depart_airport_id);
            
            // Select a random date from our upcoming dates
            start_date = this.profile.getRandomUpcomingDate();
            stop_date = new Date(start_date.getTime() + (SEATSConstants.MILLISECONDS_PER_DAY * 2));
        }
        
        // Use an existing flight so that we guaranteed to get back results
        else {
            FlightId flight_id = this.profile.getRandomFlightId();
            depart_airport_id = flight_id.getDepartAirportId();
            arrive_airport_id = flight_id.getArriveAirportId();
            
            Date flightDate = flight_id.getDepartDate(this.profile.getFlightStartDate());
            long range = Math.round(SEATSConstants.MILLISECONDS_PER_DAY * 0.5);
            start_date = new Date(flightDate.getTime() - range);
            stop_date = new Date(flightDate.getTime() + range);
            
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Using %s as look up in %s: %d / %s",
                                        flight_id, txn, flight_id.encode(), flightDate));
        }
        
        // If distance is greater than zero, then we will also get flights from nearby airports
        long distance = -1;
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_NEARBY_AIRPORT) {
            distance = SEATSConstants.DISTANCES[rng.nextInt(SEATSConstants.DISTANCES.length)];
        }
        
        Object params[] = new Object[] {
            depart_airport_id,
            arrive_airport_id,
            start_date,
            stop_date,
            distance
        };
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + txn.getExecName());
//        this.getClientHandle().callProcedure(new FindFlightsCallback(),
//                                             txn.getExecName(),
//                                             params);
        
        return (true);
    }

    // ----------------------------------------------------------------
    // FindOpenSeats
    // ----------------------------------------------------------------
    
    class FindOpenSeatsCallback extends AbstractCallback<FlightId> {
        public FindOpenSeatsCallback(FlightId f) {
            super(Transaction.FIND_OPEN_SEATS, f);
        }
        @Override
        public void clientCallbackImpl(ResultSet clientResponse[]) {
//            VoltTable[] results = clientResponse.getResults();
//            if (results.length != 1) {
//                if (LOG.isDebugEnabled()) LOG.warn("Results is " + results.length);
//                return;
//            }
//            int rowCount = results[0].getRowCount();
//            assert (rowCount <= SEATSConstants.NUM_SEATS_PER_FLIGHT) :
//                String.format("Unexpected %d open seats returned for %s", rowCount, element);
//
//            // there is some tiny probability of an empty flight .. maybe 1/(20**150)
//            // if you hit this assert (with valid code), play the lottery!
//            if (rowCount == 0) return;
//            
//            // Store pending reservations in our queue for a later transaction            
//            List<Reservation> reservations = new ArrayList<Reservation>();
//            Set<Integer> emptySeats = new HashSet<Integer>();
//            Set<CustomerId> pendingCustomers = getPendingCustomers(element);
//            while (results[0].advanceRow()) {
//                FlightId flight_id = new FlightId(results[0].getLong(0));
//                assert(flight_id.equals(element));
//                int seatnum = (int)results[0].getLong(1);
//                long airport_depart_id = flight_id.getDepartAirportId();
//                
//                // We first try to get a CustomerId based at this departure airport
//                CustomerId customer_id = SEATSWorker.this.profile.getRandomCustomerId(airport_depart_id);
//                
//                // We will go for a random one if:
//                //  (1) The Customer is already booked on this Flight
//                //  (2) We already made a new Reservation just now for this Customer
//                int tries = SEATSConstants.NUM_SEATS_PER_FLIGHT;
//                while (tries-- > 0 && (customer_id == null || pendingCustomers.contains(customer_id) || isCustomerBookedOnFlight(customer_id, flight_id))) {
//                    customer_id = SEATSWorker.this.profile.getRandomCustomerId();
//                    if (LOG.isTraceEnabled()) LOG.trace("RANDOM CUSTOMER: " + customer_id);
//                } // WHILE
//                assert(customer_id != null) :
//                    String.format("Failed to find a unique Customer to reserve for seat #%d on %s", seatnum, flight_id);
//
//                pendingCustomers.add(customer_id);
//                emptySeats.add(seatnum);
//                reservations.add(new Reservation(profile.getNextReservationId(getId()), flight_id, customer_id, (int)seatnum));
//                if (LOG.isTraceEnabled()) LOG.trace("QUEUED INSERT: " + flight_id + " / " + flight_id.encode() + " -> " + customer_id);
//            } // WHILE
//            
//            if (reservations.isEmpty() == false) {
//                int ctr = 0;
//                Collections.shuffle(reservations);
//                List<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
//                assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
//                CacheType.PENDING_INSERTS.lock.lock();
//                try {
//                    for (Reservation r : reservations) {
//                        if (cache.contains(r) == false) {
//                            cache.add(r);
//                            ctr++;
//                        }
//                    } // FOR
//                } finally {
//                    CacheType.PENDING_INSERTS.lock.unlock();
//                } // SYNCH
//                if (LOG.isDebugEnabled())
//                    LOG.debug(String.format("Stored %d pending inserts for %s [totalPendingInserts=%d]",
//                              ctr, element, cache.size()));
//            }
//            BitSet seats = getSeatsBitSet(element);
//            for (int i = 0; i < SEATSConstants.NUM_SEATS_PER_FLIGHT; i++) {
//                if (emptySeats.contains(i) == false) {
//                    seats.set(i);
//                }
//            } // FOR
        }
    }

    /**
     * Execute the FindOpenSeat procedure
     * @throws IOException
     */
    private boolean executeFindOpenSeats(Transaction txn) throws IOException {
        FlightId flight_id = this.profile.getRandomFlightId();
        assert(flight_id != null);
        
        Object params[] = new Object[] {
            flight_id.encode()
        };
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + txn.getExecName());
        
//        this.getClientHandle().callProcedure(new FindOpenSeatsCallback(flight_id),
//                                             txn.execName,
//                                             params);
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NewReservation
    // ----------------------------------------------------------------
    
    private static final Histogram<String> newReservationErrors = new Histogram<String>();
    
    class NewReservationCallback extends AbstractCallback<Reservation> {
        public NewReservationCallback(Reservation r) {
            super(Transaction.NEW_RESERVATION, r);
        }
        @Override
        public void clientCallbackImpl(ResultSet clientResponse[]) {
//            VoltTable[] results = clientResponse.getResults();
//            
//            BitSet seats = getSeatsBitSet(element.flight_id);
//            
//            // Valid NewReservation
//            if (clientResponse.getStatus() == Hstore.Status.OK) {
//                assert(results.length > 1);
//                assert(results[0].getRowCount() == 1);
//                assert(results[0].asScalarLong() == 1);
//
//                // Mark this seat as successfully reserved
//                seats.set(element.seatnum);
//
//                // Set it up so we can play with it later
//                SEATSWorker.this.requeueReservation(element);
//            }
//            // Aborted - Figure out why!
//            else if (clientResponse.getStatus() == Hstore.Status.ABORT_USER) {
//                String msg = clientResponse.getStatusString();
//                ErrorType errorType = ErrorType.getErrorType(msg);
//                
//                if (LOG.isDebugEnabled())
//                    LOG.debug(String.format("Client %02d :: NewReservation %s [ErrorType=%s] - %s",
//                                       getId(), clientResponse.getStatus(), errorType, clientResponse.getStatusString()),
//                                       clientResponse.getException());
//                
//                newReservationErrors.put(errorType.name());
//                switch (errorType) {
//                    case NO_MORE_SEATS: {
//                        seats.set(0, SEATSConstants.NUM_SEATS_PER_FLIGHT);
//                        if (LOG.isDebugEnabled())
//                            LOG.debug(String.format("FULL FLIGHT: %s", element.flight_id));                        
//                        break;
//                    }
//                    case CUSTOMER_ALREADY_HAS_SEAT: {
//                        Set<FlightId> f_ids = getCustomerBookedFlights(element.customer_id);
//                        f_ids.add(element.flight_id);
//                        if (LOG.isDebugEnabled())
//                            LOG.debug(String.format("ALREADY BOOKED: %s -> %s", element.customer_id, f_ids));
//                        break;
//                    }
//                    case SEAT_ALREADY_RESERVED: {
//                        seats.set(element.seatnum);
//                        if (LOG.isDebugEnabled())
//                            LOG.debug(String.format("ALREADY BOOKED SEAT: %s/%d -> %s",
//                                                    element.customer_id, element.seatnum, seats));
//                        break;
//                    }
//                    case INVALID_CUSTOMER_ID: {
//                        LOG.warn("Unexpected invalid CustomerId: " + element.customer_id);
//                        break;
//                    }
//                    case INVALID_FLIGHT_ID: {
//                        LOG.warn("Unexpected invalid FlightId: " + element.flight_id);
//                        break;
//                    }
//                    case UNKNOWN: {
////                        if (LOG.isDebugEnabled()) 
//                            LOG.warn(msg);
//                        break;
//                    }
//                    default: {
//                        if (LOG.isDebugEnabled()) LOG.debug("BUSTED ID: " + element.flight_id + " / " + element.flight_id.encode());
//                    }
//                } // SWITCH
//            }
        }
    }
    
    private boolean executeNewReservation(Transaction txn) throws IOException {
        Reservation reservation = null;
        BitSet seats = null;
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
        
        if (LOG.isDebugEnabled()) LOG.debug(String.format("Attempting to get a new pending insert Reservation [totalPendingInserts=%d]",
                                                 cache.size()));
        while (reservation == null) {
            Reservation r = null;
            CacheType.PENDING_INSERTS.lock.lock();
            try {
                r = cache.poll();
            } finally {
                CacheType.PENDING_INSERTS.lock.unlock();
            } // SYNCH
            if (r == null) break;
            
            seats = SEATSWorker.getSeatsBitSet(r.flight_id);
            
            if (isFlightFull(seats)) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s is full", r.flight_id));
                continue;
            }
            else if (seats.get(r.seatnum)) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("Seat #%d on %s is already booked", r.seatnum, r.flight_id));
                continue;
            }
            else if (isCustomerBookedOnFlight(r.customer_id, r.flight_id)) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s is already booked on %s", r.customer_id, r.flight_id));
                continue;
            }
            reservation = r; 
        } // WHILE
        if (reservation == null) {
            if (LOG.isDebugEnabled()) LOG.debug("Failed to find a valid pending insert Reservation\n" + this.toString());
            return (false);
        }
        
        // Generate a random price for now
        double price = 2.0 * rng.number(SEATSConstants.MIN_RESERVATION_PRICE,
                                        SEATSConstants.MAX_RESERVATION_PRICE);
        
        // Generate random attributes
        long attributes[] = new long[9];
        for (int i = 0; i < attributes.length; i++) {
            attributes[i] = rng.nextLong();
        } // FOR
        
        Object params[] = new Object[] {
                reservation.id,
                reservation.customer_id.encode(),
                reservation.flight_id.encode(),
                reservation.seatnum,
                price,
                attributes
        };
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + txn.getExecName());
//        this.getClientHandle().callProcedure(new NewReservationCallback(reservation),
//                                             txn.getExecName(),
//                                             params);
        
        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateCustomer
    // ----------------------------------------------------------------
    
    class UpdateCustomerCallback extends AbstractCallback<CustomerId> {
        public UpdateCustomerCallback(CustomerId c) {
            super(Transaction.UPDATE_CUSTOMER, c);
        }
        @Override
        public void clientCallbackImpl(ResultSet clientResponse[]) {
//            VoltTable[] results = clientResponse.getResults();
//            if (clientResponse.getStatus() == Hstore.Status.OK) {
//                assert (results.length >= 1);
//                assert (results[0].getRowCount() == 1);
////                assert (results[0].asScalarLong() == 1);
//            } else if (LOG.isDebugEnabled()) {
//                LOG.debug("UpdateCustomer " + ": " + clientResponse.getStatusString(), clientResponse.getException());
//            }
        }
    }

    private boolean executeUpdateCustomer(Transaction txn) throws SQLException {
        // Pick a random customer and then have at it!
        CustomerId customer_id = this.profile.getRandomCustomerId();
        
        Long c_id = null;
        String c_id_str = null;
        long attr0 = this.rng.nextLong();
        long attr1 = this.rng.nextLong();
        long update_ff = (rng.number(1, 100) <= SEATSConstants.PROB_UPDATE_FREQUENT_FLYER ? 1 : 0);
        
        // Update with the Customer's id as a string 
        if (rng.nextInt(100) < SEATSConstants.PROB_UPDATE_WITH_CUSTOMER_ID_STR) {
            c_id_str = Long.toString(customer_id.encode());
        }
        // Update using their Customer id
        else {
            c_id = customer_id.encode();
        }

        if (LOG.isTraceEnabled()) LOG.trace("Calling " + txn.getExecName());
        UpdateCustomer proc = this.getProcedure(UpdateCustomer.class);
        proc.run(conn, c_id, c_id_str, update_ff, attr0, attr1);
        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateReservation
    // ----------------------------------------------------------------
    
    class UpdateReservationCallback extends AbstractCallback<Reservation> {
        public UpdateReservationCallback(Reservation r) {
            super(Transaction.UPDATE_RESERVATION, r);
        }
        @Override
        public void clientCallbackImpl(ResultSet clientResponse[]) {
//            if (clientResponse.getStatus() == Hstore.Status.OK) {
//                assert (clientResponse.getResults().length == 1);
//                assert (clientResponse.getResults()[0].getRowCount() == 1);
//                assert (clientResponse.getResults()[0].asScalarLong() == 1 ||
//                        clientResponse.getResults()[0].asScalarLong() == 0);
//                
//                SEATSWorker.this.requeueReservation(element);
//            }
        }
    }

    private boolean executeUpdateReservation(Transaction txn) throws IOException {
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_UPDATES;
        
        // Pull off the first pending seat change and throw that ma at the server
        Reservation r = null;
        CacheType.PENDING_UPDATES.lock.lock();
        try {
            r = cache.poll();
        } finally {
            CacheType.PENDING_UPDATES.lock.unlock();
        } // SYNCH
        if (r == null) {
            return (false);
        }
        
        // Pick a random reservation id
        long value = rng.number(1, 1 << 20);
        long attribute_idx = rng.nextInt(UpdateReservation.NUM_UPDATES);

        Object params[] = new Object[] {
                r.id,
                r.flight_id.encode(),
                r.customer_id.encode(),
                r.seatnum,
                attribute_idx,
                value
        };
        
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + txn.getExecName());
//        this.getClientHandle().callProcedure(new UpdateReservationCallback(r),
//                                             txn.getExecName(), 
//                                             params);
        return (true);
    }

}