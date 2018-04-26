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


package com.oltpbenchmark.benchmarks.auctionmark;

import java.sql.Timestamp;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.auctionmark.exceptions.DuplicateItemIdException;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.CloseAuctions;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.GetItem;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.GetUserInfo;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.NewBid;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.NewComment;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.NewCommentResponse;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.NewFeedback;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.NewItem;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.NewPurchase;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.UpdateItem;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.GlobalAttributeValueId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemInfo;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemCommentResponse;
import com.oltpbenchmark.benchmarks.auctionmark.util.UserId;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.SQLUtil;

public class AuctionMarkWorker extends Worker<AuctionMarkBenchmark> {
    private static final Logger LOG = Logger.getLogger(AuctionMarkWorker.class);

    // -----------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    protected final AuctionMarkProfile profile;
    
    private final AtomicBoolean closeAuctions_flag = new AtomicBoolean();
    
    private final Thread closeAuctions_checker;

    protected static final Map<Long, Integer> ip_id_cntrs = new HashMap<Long, Integer>(); 
    
    // --------------------------------------------------------------------
    // CLOSE_AUCTIONS CHECKER
    // --------------------------------------------------------------------
    private class CloseAuctionsChecker extends Thread {
        {
            this.setDaemon(true);
        }
        @Override
        public void run() {
            Thread.currentThread().setName(this.getClass().getSimpleName());
            
            TransactionTypes txnTypes = AuctionMarkWorker.this.getWorkloadConfiguration().getTransTypes(); 
            TransactionType txnType = txnTypes.getType(CloseAuctions.class);
            assert(txnType != null) : txnTypes;
            
            Procedure proc = AuctionMarkWorker.this.getProcedure(txnType);
            assert(proc != null);
            
            long sleepTime = AuctionMarkConstants.CLOSE_AUCTIONS_INTERVAL / AuctionMarkConstants.TIME_SCALE_FACTOR;
            while (true) {
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Sleeping for %d seconds", sleepTime));

                // Always sleep until the next time that we need to check
                try {
                    Thread.sleep(sleepTime * AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
//                assert(AuctionMarkWorker.this.closeAuctions_flag.get() == false);
                
                if (AuctionMarkConstants.CLOSE_AUCTIONS_SEPARATE_THREAD) {
                    if (LOG.isDebugEnabled())
                        LOG.debug(String.format("Executing %s in separate thread", txnType));
                    try {
                        executeCloseAuctions((CloseAuctions)proc);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    AuctionMarkWorker.this.closeAuctions_flag.set(true);
                    if (LOG.isDebugEnabled())
                        LOG.debug(String.format("Marked ready flag for %s", txnType));
                }
            } // WHILE
        }
    }
    
    // --------------------------------------------------------------------
    // TXN PARAMETER GENERATOR
    // --------------------------------------------------------------------
    public interface AuctionMarkParamGenerator {
        /**
         * Returns true if the client will be able to successfully generate a new transaction call
         * The client passes in the current BenchmarkProfile handle and an optional VoltTable. This allows
         * you to invoke one txn using the output of a previously run txn.
         * Note that this is not thread safe, so you'll need to combine the call to this with generate()
         * in a single synchronization block.
         * @param client
         * @return
         */
        public boolean canGenerateParam(AuctionMarkWorker client);
    }
    
    // --------------------------------------------------------------------
    // BENCHMARK TRANSACTIONS
    // --------------------------------------------------------------------
    public enum Transaction {
        // ====================================================================
        // CloseAuctions
        // ====================================================================
        CloseAuctions(CloseAuctions.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
//                if (AuctionMarkConstants.ENABLE_CLOSE_AUCTIONS && client.getId() == 0) {
//                    // If we've never checked before, then we'll want to do that now
//                    if (client.profile.hasLastCloseAuctionsTime() == false) return (true);
//
//                    // Otherwise 
//                    return (run);
//                }
                return (false); // Use CloseAuctionsChecker
            }
        }),
        // ====================================================================
        // GetItem
        // ====================================================================
        GetItem(GetItem.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.getAvailableItemsCount() > 0);
            }
        }),
        // ====================================================================
        // GetUserInfo
        // ====================================================================
        GetUserInfo(GetUserInfo.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (true);
            }
        }),
        // ====================================================================
        // NewBid
        // ====================================================================
        NewBid(NewBid.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.getAllItemsCount() > 0);
            }
        }),
        // ====================================================================
        // NewComment
        // ====================================================================
        NewComment(NewComment.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.getCompleteItemsCount() > 0);
            }
        }),
        // ====================================================================
        // NewCommentResponse
        // ====================================================================
        NewCommentResponse(NewCommentResponse.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.pending_commentResponses.isEmpty() == false);
            }
        }),
        // ====================================================================
        // NewFeedback
        // ====================================================================
        NewFeedback(NewFeedback.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.getCompleteItemsCount() > 0);
            }
        }),
        // ====================================================================
        // NewItem
        // ====================================================================
        NewItem(NewItem.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (true);
            }
        }),
        // ====================================================================
        // NewPurchase
        // ====================================================================
        NewPurchase(NewPurchase.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.getWaitForPurchaseItemsCount() > 0);
            }
        }),
        // ====================================================================
        // UpdateItem
        // ====================================================================
        UpdateItem(UpdateItem.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.getAvailableItemsCount() > 0);
            }
        }), 
        ;
        
        /**
         * Constructor
         * @param weight The execution frequency weight for this txn 
         * @param generator
         */
        private Transaction(Class<? extends Procedure> procClass, AuctionMarkParamGenerator generator) {
            this.procClass = procClass;
            this.generator = generator;
        }

        public final Class<? extends Procedure> procClass;
        public final AuctionMarkParamGenerator generator;
        
        protected static final Map<Class<? extends Procedure>, Transaction> class_lookup = new HashMap<Class<? extends Procedure>, Transaction>();
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<String, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name(), vt);
                Transaction.class_lookup.put(vt.procClass, vt);
            }
        }
        
        public static Transaction get(Integer idx) {
            assert(idx >= 0);
            return (Transaction.idx_lookup.get(idx));
        }

        public static Transaction get(Class<? extends Procedure> clazz) {
            return (Transaction.class_lookup.get(clazz));
        }
        public static Transaction get(String name) {
            return (Transaction.name_lookup.get(name));
        }
        
        /**
         * This will return true if we can call a new transaction for this procedure
         * A txn can be called if we can generate all of the parameters we need
         * @return
         */
        public boolean canExecute(AuctionMarkWorker client) {
            if (LOG.isDebugEnabled()) LOG.debug("Checking whether we can execute " + this + " now");
            return this.generator.canGenerateParam(client);
        }
    }

    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    /**
     * Constructor
     * @param args
     */
    public AuctionMarkWorker(int id, AuctionMarkBenchmark benchmark) {
        super(benchmark, id);
        this.profile = new AuctionMarkProfile(benchmark, benchmark.getRandomGenerator());
        
        boolean needCloseAuctions = (AuctionMarkConstants.CLOSE_AUCTIONS_ENABLE && id == 0);
        this.closeAuctions_flag.set(needCloseAuctions);
        if (needCloseAuctions) {
            this.closeAuctions_checker = new CloseAuctionsChecker(); 
        } else {
            this.closeAuctions_checker = null;
        }
    }
    
    protected AuctionMarkProfile getProfile() {
        return (this.profile);
    }
    
    @Override
    protected void initialize() {
        // Load BenchmarkProfile
        try {
            profile.loadProfile(this);
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to initialize AuctionMarkWorker", ex);
        }
        if (this.closeAuctions_checker != null) this.closeAuctions_checker.start();
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("AuctionMarkProfile %03d\n%s", this.getId(), profile));
    }
    
    @Override
    protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {
        // We need to subtract the different between this and the profile's start time,
        // since that accounts for the time gap between when the loader started and when the client start.
        // Otherwise, all of our cache date will be out dated if it took a really long time
        // to load everything up. Again, in order to keep things in synch, we only want to
        // set this on the first call to runOnce(). This will account for start a bunch of
        // clients on multiple nodes but then having to wait until they're all up and running
        // before starting the actual benchmark run.
        if (profile.hasClientStartTime() == false) profile.setAndGetClientStartTime();
      
        // Always update the current timestamp
        profile.updateAndGetCurrentTime();
          
        Transaction txn = null; 
        
        // Always check if we need to want to run CLOSE_AUCTIONS
        // We only do this from the first client
        if (AuctionMarkConstants.CLOSE_AUCTIONS_SEPARATE_THREAD == false &&
            closeAuctions_flag.compareAndSet(true, false)) {
            txn = Transaction.CloseAuctions;
            TransactionTypes txnTypes = this.getWorkloadConfiguration().getTransTypes(); 
            txnType = txnTypes.getType(txn.procClass);
            assert(txnType != null) : txnTypes;
        } else {
            txn = Transaction.get(txnType.getProcedureClass());
            assert(txn != null) :
                "Failed to get Transaction handle for " + txnType.getProcedureClass().getSimpleName(); 
            if (txn.canExecute(this) == false) {
                if (LOG.isDebugEnabled())
                    LOG.warn("Unable to execute " + txn + " because it is not ready");
                return (TransactionStatus.RETRY_DIFFERENT);
            }
        }
        
        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        assert(proc != null);
        if (LOG.isTraceEnabled())
            LOG.trace(txnType + " -> " + txn + " -> " + txnType.getProcedureClass() + " -> " + proc);
        
        boolean ret = false;
        switch (txn) {
            case CloseAuctions:
                ret = executeCloseAuctions((CloseAuctions)proc);
                break;
            case GetItem:
                ret = executeGetItem((GetItem)proc);
                break;
            case GetUserInfo:
                ret = executeGetUserInfo((GetUserInfo)proc);
                break;
            case NewBid:
                ret = executeNewBid((NewBid)proc);
                break;
            case NewComment:
                ret = executeNewComment((NewComment)proc);
                break;
            case NewCommentResponse:
                ret = executeNewCommentResponse((NewCommentResponse)proc);
                break;
            case NewFeedback:
                ret = executeNewFeedback((NewFeedback)proc);
                break;
            case NewItem:
                ret = executeNewItem((NewItem)proc);
                break;
            case NewPurchase:
                ret = executeNewPurchase((NewPurchase)proc);
                break;
            case UpdateItem:
                ret = executeUpdateItem((UpdateItem)proc);
                break;
            default:
                assert(false) : "Unexpected transaction: " + txn; 
        } // SWITCH
//        assert(ret);
        if (ret && LOG.isDebugEnabled())
            LOG.debug("Executed a new invocation of " + txn);
        
        return (TransactionStatus.SUCCESS);
    }
    
    /**
     * For the given VoltTable that contains ITEM records, process the current
     * row of that table and update the benchmark profile based on item information
     * stored in that row. 
     * @param vt
     * @return
     * @see AuctionMarkConstants.ITEM_COLUMNS
     * @see CloseAuctions
     * @see GetItem
     * @see GetUserInfo
     * @see NewBid
     * @see NewItem
     * @see NewPurchase
     */
    @SuppressWarnings("unused")
    public ItemId processItemRecord(Object row[]) {
        int col = 0;
        ItemId i_id = new ItemId(SQLUtil.getLong(row[col++]));  // i_id
        long i_u_id = SQLUtil.getLong(row[col++]);              // i_u_id
        String i_name = (String)row[col++];                     // i_name
        double i_current_price = SQLUtil.getDouble(row[col++]); // i_current_price
        long i_num_bids = SQLUtil.getLong(row[col++]);          // i_num_bids
        Timestamp i_end_date = SQLUtil.getTimestamp(row[col++]);// i_end_date
        if (i_end_date == null) throw new RuntimeException("DJELLEL IS THE MAN! --> " + row[col-1] + " / " + row[col-1].getClass());
        
        Integer temp = SQLUtil.getInteger(row[col++]);
        if (temp == null) throw new RuntimeException("DJELLEL IS STILL THE MAN! --> " + row[col-1] + " / " + row[col-1].getClass());
        ItemStatus i_status = ItemStatus.get(temp); // i_status
        
        ItemInfo itemInfo = new ItemInfo(i_id, i_current_price, i_end_date, (int)i_num_bids);
        itemInfo.status = i_status;
        
        UserId sellerId = new UserId(i_u_id);
        assert (i_id.getSellerId().equals(sellerId));
         
        ItemStatus qtype = profile.addItemToProperQueue(itemInfo, false);
    
        return (i_id);
    }
    
    public Timestamp[] getTimestampParameterArray() {
        return new Timestamp[] { profile.getLoaderStartTime(),
                                 profile.getClientStartTime() };
    }
    
    // ----------------------------------------------------------------
    // CLOSE_AUCTIONS
    // ----------------------------------------------------------------
    
    protected boolean executeCloseAuctions(CloseAuctions proc) throws SQLException {
        if (LOG.isDebugEnabled())
            LOG.debug("Executing " + proc);
        Timestamp benchmarkTimes[] = this.getTimestampParameterArray();
        Timestamp startTime = profile.getLastCloseAuctionsTime();
        Timestamp endTime = profile.updateAndGetLastCloseAuctionsTime();
        
        List<Object[]> results = proc.run(conn, benchmarkTimes, startTime, endTime);
        conn.commit();
        
        assert(null != results);
        for (Object row[] : results) {
            ItemId itemId = this.processItemRecord(row);
            assert(itemId != null);
        } // WHILE
        profile.updateItemQueues();
        
        if (LOG.isDebugEnabled())
            LOG.debug("Finished " + proc);
        return (true);
    }
    
    // ----------------------------------------------------------------
    // GetItem
    // ----------------------------------------------------------------
    
    protected boolean executeGetItem(GetItem proc) throws SQLException {
        Timestamp benchmarkTimes[] = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomAvailableItemId();
        
        Object results[][] = proc.run(conn, benchmarkTimes, itemInfo.itemId.encode(),
                                                            itemInfo.getSellerId().encode());
        conn.commit();
        
        // The first row will have our item data that we want
        // We don't care about the user information...
        ItemId itemId = this.processItemRecord(results[0]);
        assert(itemId != null);
        
        return (true);
    }
    
    // ----------------------------------------------------------------
    // GetUserInfo
    // ----------------------------------------------------------------
    
    protected boolean executeGetUserInfo(GetUserInfo proc) throws SQLException {
        Timestamp benchmarkTimes[] = this.getTimestampParameterArray();
        UserId userId = profile.getRandomBuyerId();
        int rand;
        
        // USER_FEEDBACK records
        rand = profile.rng.number(0, 100);
        boolean get_feedback = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_FEEDBACK); 

        // ITEM_COMMENT records
        rand = profile.rng.number(0, 100);
        boolean get_comments = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_COMMENTS);
        
        // Seller ITEM records
        rand = profile.rng.number(0, 100);
        boolean get_seller_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_SELLER_ITEMS); 

        // Buyer ITEM records
        rand = profile.rng.number(0, 100);
        boolean get_buyer_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_BUYER_ITEMS);
        
        // USER_WATCH records
        rand = profile.rng.number(0, 100);
        boolean get_watched_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_WATCHED_ITEMS); 
        
        List<Object[]>[] results = proc.run(conn, benchmarkTimes, userId.encode(),
                                                                  get_feedback,
                                                                  get_comments,
                                                                  get_seller_items,
                                                                  get_buyer_items,
                                                                  get_watched_items);
        conn.commit();
        
        List<Object[]> vt = null;
        int idx = 0;
      
        // USER
        vt = results[idx++];
        assert(vt != null);
        assert(vt.size() > 0) :
            "Failed to find user information for " + userId + " / " + userId.encode();
          
        // USER_FEEDBACK
        if (get_feedback) {
            vt = results[idx];
            assert(vt != null);
        }
        idx++;
        
        // ITEM_COMMENTS
        if (get_comments) {
            vt = results[idx];
            assert(vt != null);
            Long vals[] = new Long[3];
            int cols[] = { AuctionMarkConstants.ITEM_COLUMNS.length + 1, 1, 2 };
            for (Object row[] : vt) {
                boolean valid = true;
                for (int i = 0; i < cols.length; i++) {
                    if (row[i] == null) {
                        valid = false;
                        break;
                    }
                    vals[i] = SQLUtil.getLong(row[i]);
                    if (vals[i] == null) {
                        valid = false;
                        break;
                    }
                } // FOR
                if (valid) {
                    ItemCommentResponse cr = new ItemCommentResponse(vals[0], vals[1], vals[2]);
                    profile.addPendingItemCommentResponse(cr);
                }
            } // FOR
        }
        idx++;
      
        // ITEM Result Tables
        for ( ; idx < results.length; idx++) {
            vt = results[idx];
            if (vt == null) continue;
            for (Object row[] : vt) {
                ItemId itemId = this.processItemRecord(row);
                assert(itemId != null);
            } // FOR
        } // FOR
        
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NewBid
    // ----------------------------------------------------------------
    
    protected boolean executeNewBid(NewBid proc) throws SQLException {
        Timestamp benchmarkTimes[] = this.getTimestampParameterArray();
        ItemInfo itemInfo = null;
        UserId sellerId;
        UserId buyerId;
        double bid;
        double maxBid;
        
        boolean has_available = (profile.getAvailableItemsCount() > 0);
        boolean has_ending = (profile.getEndingSoonItemsCount() > 0);
        boolean has_waiting = (profile.getWaitForPurchaseItemsCount() > 0);
        boolean has_completed = (profile.getCompleteItemsCount() > 0); 
        
        // Some NewBids will be for items that have already ended.
        // This will simulate somebody trying to bid at the very end but failing
        if ((has_waiting || has_completed) &&
            (profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_NEWBID_CLOSED_ITEM || has_available == false)) {
            if (has_waiting) {
                itemInfo = profile.getRandomWaitForPurchaseItem();
                assert(itemInfo != null) : "Failed to get WaitForPurchase itemInfo [" + profile.getWaitForPurchaseItemsCount() + "]";
            } else {
                itemInfo = profile.getRandomCompleteItem();
                assert(itemInfo != null) : "Failed to get Completed itemInfo [" + profile.getCompleteItemsCount() + "]";
            }
            sellerId = itemInfo.getSellerId();
            buyerId = profile.getRandomBuyerId(sellerId);
            
            // The bid/maxBid do not matter because they won't be able to actually
            // update the auction
            bid = profile.rng.nextDouble();
            maxBid = bid + 100;
        }
        
        // Otherwise we want to generate information for a real bid
        else {
            assert(has_available || has_ending);
            // 50% of NewBids will be for items that are ending soon
            if ((has_ending && profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_NEWBID_CLOSED_ITEM) || has_available == false) {
                itemInfo = profile.getRandomEndingSoonItem(true);
            }
            if (itemInfo == null) {
                itemInfo = profile.getRandomAvailableItem(true);
            }
            if (itemInfo == null) {
                itemInfo = profile.getRandomItem();
            }
            
            sellerId = itemInfo.getSellerId();
            buyerId = profile.getRandomBuyerId(sellerId);
            
            double currentPrice = itemInfo.getCurrentPrice();
            bid = profile.rng.fixedPoint(2, currentPrice, currentPrice * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP / 2)));
            maxBid = profile.rng.fixedPoint(2, bid, (bid * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP / 2))));
        }

        Object results[] = proc.run(conn, benchmarkTimes, itemInfo.itemId.encode(),
                                                          sellerId.encode(),
                                                          buyerId.encode(),
                                                          maxBid,
                                                          itemInfo.endDate);
        conn.commit();
        
        ItemId itemId = this.processItemRecord(results);
        assert(itemId != null);
    
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NewComment
    // ----------------------------------------------------------------
    
    protected boolean executeNewComment(NewComment proc) throws SQLException {
        Timestamp benchmarkTimes[] = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomCompleteItem();
        UserId sellerId = itemInfo.getSellerId();
        UserId buyerId = profile.getRandomBuyerId(sellerId);
        String question = profile.rng.astring(AuctionMarkConstants.ITEM_COMMENT_LENGTH_MIN,
                                              AuctionMarkConstants.ITEM_COMMENT_LENGTH_MAX);
        
        Object results[] = proc.run(conn, benchmarkTimes,
                                          itemInfo.itemId.encode(),
                                          sellerId.encode(),
                                          buyerId.encode(),
                                          question);
        conn.commit();
        assert(results != null);
        
        profile.pending_commentResponses.add(new ItemCommentResponse(SQLUtil.getLong(results[0]),
                                                                     SQLUtil.getLong(results[1]),
                                                                     SQLUtil.getLong(results[2])));
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NewCommentResponse
    // ----------------------------------------------------------------
    
    protected boolean executeNewCommentResponse(NewCommentResponse proc) throws SQLException {
        Timestamp benchmarkTimes[] = this.getTimestampParameterArray();
        int idx = profile.rng.nextInt(profile.pending_commentResponses.size());
        ItemCommentResponse cr = profile.pending_commentResponses.remove(idx);
        assert(cr != null);
        
        long commentId = cr.commentId.longValue();
        ItemId itemId = new ItemId(cr.itemId.longValue());
        UserId sellerId = itemId.getSellerId();
        assert(sellerId.encode() == cr.sellerId);
        String response = profile.rng.astring(AuctionMarkConstants.ITEM_COMMENT_LENGTH_MIN,
                                              AuctionMarkConstants.ITEM_COMMENT_LENGTH_MAX);

        proc.run(conn, benchmarkTimes, itemId.encode(),
                                       sellerId.encode(),
                                       commentId,
                                       response);
        conn.commit();
        
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NewFeedback
    // ----------------------------------------------------------------
    
    protected boolean executeNewFeedback(NewFeedback proc) throws SQLException {
        Timestamp benchmarkTimes[] = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomCompleteItem();
        UserId sellerId = itemInfo.getSellerId();
        UserId buyerId = profile.getRandomBuyerId(sellerId);
        long rating = (long) profile.rng.number(-1, 1);
        String feedback = profile.rng.astring(10, 80);
        
        long user_id;
        long from_id;
        if (profile.rng.nextBoolean()) {
            user_id = sellerId.encode();
            from_id = buyerId.encode();
        } else {
            user_id = buyerId.encode();
            from_id = sellerId.encode();
        }
        
        proc.run(conn, benchmarkTimes, user_id,
                                       itemInfo.itemId.encode(),
                                       sellerId.encode(),
                                       from_id,
                                       rating,
                                       feedback);
        conn.commit();
        
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NewItem
    // ----------------------------------------------------------------

    protected boolean executeNewItem(NewItem proc) throws SQLException {
        Timestamp benchmarkTimes[] = this.getTimestampParameterArray();
        UserId sellerId = profile.getRandomSellerId(this.getId());
        ItemId itemId = profile.getNextItemId(sellerId);

        String name = profile.rng.astring(6, 32);
        String description = profile.rng.astring(50, 255);
        long categoryId = profile.getRandomCategoryId();

        Double initial_price = (double) profile.randomInitialPrice.nextInt();
        String attributes = profile.rng.astring(50, 255);

        int numAttributes = profile.randomNumAttributes.nextInt();
        List<GlobalAttributeValueId> gavList = new ArrayList<GlobalAttributeValueId>(numAttributes);
        for (int i = 0; i < numAttributes; i++) {
            GlobalAttributeValueId gav_id = profile.getRandomGlobalAttributeValue();
            if (!gavList.contains(gav_id)) gavList.add(gav_id);
        } // FOR

        long[] gag_ids = new long[gavList.size()];
        long[] gav_ids = new long[gavList.size()];
        for (int i = 0, cnt = gag_ids.length; i < cnt; i++) {
            GlobalAttributeValueId gav_id = gavList.get(i);
            gag_ids[i] = gav_id.getGlobalAttributeGroup().encode();
            gav_ids[i] = gav_id.encode();
        } // FOR

        int numImages = profile.randomNumImages.nextInt();
        String[] images = new String[numImages];
        for (int i = 0; i < numImages; i++) {
            images[i] = profile.rng.astring(20, 100);
        } // FOR

        long duration = profile.randomDuration.nextInt();

        Object results[] = null;
        try {
            long itemIdEncoded = itemId.encode();
            results = proc.run(conn, benchmarkTimes, itemIdEncoded, sellerId.encode(),
                                                     categoryId, name, description,
                                                     duration, initial_price, attributes,
                                                     gag_ids, gav_ids, images);
        } catch (DuplicateItemIdException ex) {
            profile.seller_item_cnt.set(sellerId, ex.getItemCount());
            throw ex;
        }
        conn.commit();
                       
        itemId = this.processItemRecord(results);
        assert(itemId != null);
        
        return (true);
    }

    // ----------------------------------------------------------------
    // NewPurchase
    // ----------------------------------------------------------------
    
    protected boolean executeNewPurchase(NewPurchase proc) throws SQLException {
        Timestamp benchmarkTimes[] = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomWaitForPurchaseItem();
        long encodedItemId = itemInfo.itemId.encode();
        UserId sellerId = itemInfo.getSellerId();
        double buyer_credit = 0d;

	Integer ip_id_cnt = ip_id_cntrs.get(new Long(encodedItemId));
	if (ip_id_cnt == null) {
	    ip_id_cnt = new Integer(0);
	}
	
        long ip_id = AuctionMarkUtil.getUniqueElementId(encodedItemId,
                                                        ip_id_cnt.intValue());
	ip_id_cntrs.put(encodedItemId, (ip_id_cnt < 127) ? ip_id_cnt + 1 : 0);
        
        // Whether the buyer will not have enough money
        if (itemInfo.hasCurrentPrice()) {
            if (profile.rng.number(1, 100) < AuctionMarkConstants.PROB_NEWPURCHASE_NOT_ENOUGH_MONEY) {
                buyer_credit = -1 * itemInfo.getCurrentPrice();
            } else {
                buyer_credit = itemInfo.getCurrentPrice();
                itemInfo.status = ItemStatus.CLOSED;
            }
        }
        
        Object results[] = proc.run(conn, benchmarkTimes, encodedItemId,
                                                          sellerId.encode(),
                                                          ip_id,
				                          buyer_credit);
        conn.commit();
        
        ItemId itemId = this.processItemRecord(results);
        assert(itemId != null);
        
        return (true);
    }
    
    // ----------------------------------------------------------------
    // UpdateItem
    // ----------------------------------------------------------------
    
    protected boolean executeUpdateItem(UpdateItem proc) throws SQLException {
        Timestamp benchmarkTimes[] = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomAvailableItemId();
        UserId sellerId = itemInfo.getSellerId();
        String description = profile.rng.astring(50, 255);
        
        boolean delete_attribute = false;
        long add_attribute[] = {
            -1,
            -1
        };
        
        // Delete ITEM_ATTRIBUTE
        if (profile.rng.number(1, 100) < AuctionMarkConstants.PROB_UPDATEITEM_DELETE_ATTRIBUTE) {
            delete_attribute = true;
        }
        // Add ITEM_ATTRIBUTE
        else if (profile.rng.number(1, 100) < AuctionMarkConstants.PROB_UPDATEITEM_ADD_ATTRIBUTE) {
            GlobalAttributeValueId gav_id = profile.getRandomGlobalAttributeValue();
            assert(gav_id != null);
            add_attribute[0] = gav_id.getGlobalAttributeGroup().encode();
            add_attribute[1] = gav_id.encode();
        }
        
        proc.run(conn, benchmarkTimes, itemInfo.itemId.encode(),
                                       sellerId.encode(),
                                       description,
                                       delete_attribute,
                                       add_attribute);
        conn.commit();
        
        return (true);
    }
}
