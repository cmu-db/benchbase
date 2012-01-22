package com.oltpbenchmark.benchmarks.auctionmark;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants.ItemStatus;
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
import com.oltpbenchmark.benchmarks.auctionmark.util.GlobalAttributeValueId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemInfo;
import com.oltpbenchmark.benchmarks.auctionmark.util.UserId;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.StringUtil;

public class AuctionMarkWorker extends Worker {
    private static final Logger LOG = Logger.getLogger(AuctionMarkLoader.class);

    // -----------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    protected final AuctionMarkProfile profile;
    
    /**
     * TODO
     */
    private final Map<UserId, Integer> seller_item_cnt = new HashMap<UserId, Integer>();

    /**
     * TODO
     */
    private final List<long[]> pending_commentResponse = Collections.synchronizedList(new ArrayList<long[]>());
    
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
        // CLOSE_AUCTIONS
        // ====================================================================
        CLOSE_AUCTIONS(CloseAuctions.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                if (AuctionMarkConstants.ENABLE_CLOSE_AUCTIONS && client.getId() == 0) {
                    // If we've never checked before, then we'll want to do that now
                    if (client.profile.hasLastCloseAuctionsTime() == false) return (true);

                    // Otherwise check whether enough time has passed since the last time we checked
                    Date lastCheckWinningBidTime = client.profile.getLastCloseAuctionsTime();
                    Date currentTime = client.profile.getCurrentTime();
                    long time_elapsed = Math.round((currentTime.getTime() - lastCheckWinningBidTime.getTime()) / 1000.0);
                    if (LOG.isDebugEnabled()) 
                        LOG.debug(String.format("%s [start=%s, current=%s, elapsed=%d]",
                                  Transaction.CLOSE_AUCTIONS, client.profile.getBenchmarkStartTime(), currentTime, time_elapsed));
                    if (time_elapsed > AuctionMarkConstants.INTERVAL_CLOSE_AUCTIONS) return (true);
                }
                return (false);
            }
        }),
        // ====================================================================
        // GET_ITEM
        // ====================================================================
        GET_ITEM(GetItem.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.getAvailableItemsCount() > 0);
            }
        }),
        // ====================================================================
        // GET_USER_INFO
        // ====================================================================
        GET_USER_INFO(GetUserInfo.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (true);
            }
        }),
        // ====================================================================
        // NEW_BID
        // ====================================================================
        NEW_BID(NewBid.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.getAllItemsCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_COMMENT
        // ====================================================================
        NEW_COMMENT(NewComment.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.getCompleteItemsCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_COMMENT_RESPONSE
        // ====================================================================
        NEW_COMMENT_RESPONSE(NewCommentResponse.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.pending_commentResponse.isEmpty() == false);
            }
        }),
        // ====================================================================
        // NEW_FEEDBACK
        // ====================================================================
        NEW_FEEDBACK(NewFeedback.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.getCompleteItemsCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_ITEM
        // ====================================================================
        NEW_ITEM(NewItem.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (true);
            }
        }),
        // ====================================================================
        // NEW_PURCHASE
        // ====================================================================
        NEW_PURCHASE(NewPurchase.class, new AuctionMarkParamGenerator() {
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                return (client.profile.getWaitForPurchaseItemsCount() > 0);
            }
        }),
        // ====================================================================
        // UPDATE_ITEM
        // ====================================================================
        UPDATE_ITEM(UpdateItem.class, new AuctionMarkParamGenerator() {
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
            this.displayName = StringUtil.title(this.name().replace("_", " "));
            this.callName = this.displayName.replace(" ", "");
            this.generator = generator;
        }

        public final Class<? extends Procedure> procClass;
        public final String displayName;
        public final String callName;
        public final AuctionMarkParamGenerator generator;
        
        protected static final Map<Class<? extends Procedure>, Transaction> class_lookup = new HashMap<Class<? extends Procedure>, Transaction>();
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<String, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name().toLowerCase().intern(), vt);
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
            return (Transaction.name_lookup.get(name.toLowerCase().intern()));
        }
        
        public String getDisplayName() {
            return (this.displayName);
        }
        
        public String getCallName() {
            return (this.callName);
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
        super(id, benchmark);

        // BenchmarkProfile
        profile = new AuctionMarkProfile(benchmark, benchmark.getRandomGenerator());
        try {
            profile.loadProfile(this.conn);
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to initialize AuctionMarkWorker", ex);
        }
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
        if (Transaction.CLOSE_AUCTIONS.canExecute(this)) {
            txn = Transaction.CLOSE_AUCTIONS;
            assert(txnType != null);
        } else {
            txn = Transaction.get(txnType.getProcedureClass());
            if (txn.canExecute(this) == false) {
                if (LOG.isTraceEnabled())
                    LOG.warn("Unable to execute " + txn + " because it is not ready");
                return (TransactionStatus.RETRY);
            }
        }
        
        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        assert(proc != null);
        boolean ret = false;
        switch (txn) {
            case CLOSE_AUCTIONS:
                ret = executeCloseAuctions((CloseAuctions)proc);
                break;
            case GET_ITEM:
                ret = executeGetItem((GetItem)proc);
                break;
            case GET_USER_INFO:
                ret = executeGetUserInfo((GetUserInfo)proc);
                break;
            case NEW_BID:
                ret = executeNewBid((NewBid)proc);
                break;
            case NEW_COMMENT:
                ret = executeNewComment((NewComment)proc);
                break;
            case NEW_COMMENT_RESPONSE:
                ret = executeNewCommentResponse((NewCommentResponse)proc);
                break;
            case NEW_FEEDBACK:
                ret = executeNewFeedback((NewFeedback)proc);
                break;
            case NEW_ITEM:
                ret = executeNewItem((NewItem)proc);
                break;
            case NEW_PURCHASE:
                ret = executeNewPurchase((NewPurchase)proc);
                break;
            case UPDATE_ITEM:
                ret = executeUpdateItem((UpdateItem)proc);
                break;
            default:
                assert(false) : "Unexpected transaction: " + txn; 
        } // SWITCH
        return (TransactionStatus.SUCCESS);
    }
    
    /**
     * For the given VoltTable that contains ITEM records, process the current
     * row of that table and update the benchmark profile based on item information
     * stored in that row. 
     * @param vt
     * @return
     * @see CloseAuctions
     * @see GetUserInfo
     * @see NewBid
     * @see NewItem
     * @see NewPurchase
     */
    public ItemId processItemRecord(Object row[]) {
        int col = 0;
        ItemId i_id = new ItemId((Long)row[col++]);             // i_id
        long i_u_id = (Long)row[col++];                         // i_u_id
        double i_current_price = (Double)row[col++];            // i_current_price
        long i_num_bids = (Long)row[col++];                     // i_num_bids
        Date i_end_date = (Date)row[col++];                     // i_end_date
        ItemStatus i_status = ItemStatus.get((Long)row[col++]); // i_status
        
        ItemInfo itemInfo = new ItemInfo(i_id, i_current_price, i_end_date, (int)i_num_bids);
        itemInfo.status = i_status;
        
        UserId sellerId = new UserId(i_u_id);
        assert (i_id.getSellerId().equals(sellerId));
         
        ItemStatus qtype = profile.addItemToProperQueue(itemInfo, false);
//        this.updated.put(qtype);
    
        return (i_id);
    }
    
    public ItemId getNextItemId(UserId seller_id) {
        Integer cnt = this.seller_item_cnt.get(seller_id);
        if (cnt == null || cnt == 0) {
            cnt = (int)seller_id.getItemCount();
        }
        this.seller_item_cnt.put(seller_id, ++cnt);
        return (new ItemId(seller_id, cnt));
    }
    
    public Date[] getTimestampParameterArray() {
        return new Date[] { profile.getBenchmarkStartTime(),
                            profile.getClientStartTime() };
    }
    
    // ----------------------------------------------------------------
    // CLOSE_AUCTIONS
    // ----------------------------------------------------------------
    
    protected boolean executeCloseAuctions(CloseAuctions proc) throws SQLException {
        Date benchmarkTimes[] = this.getTimestampParameterArray();
        Date startTime = profile.getLastCloseAuctionsTime();
        Date endTime = profile.updateAndGetLastCloseAuctionsTime();
        
        List<Object[]> results = proc.run(conn, benchmarkTimes, startTime, endTime);
        conn.commit();
        
        assert (null != results && results.size() > 0);
        for (Object row[] : results) {
            ItemId itemId = this.processItemRecord(row);
            assert(itemId != null);
        } // WHILE
        if (LOG.isDebugEnabled()) LOG.debug(super.toString());
        profile.updateItemQueues();
        
        return (true);
    }
    
    // ----------------------------------------------------------------
    // GET_ITEM
    // ----------------------------------------------------------------
    
    protected boolean executeGetItem(GetItem proc) throws SQLException {
        Date benchmarkTimes[] = this.getTimestampParameterArray();
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
    // GET_USER_INFO
    // ----------------------------------------------------------------
    
    protected boolean executeGetUserInfo(GetUserInfo proc) throws SQLException {
        Date benchmarkTimes[] = this.getTimestampParameterArray();
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
        assert(vt.size() > 0);
          
        // USER_FEEDBACK
        if (get_feedback) {
            vt = results[idx];
            assert(vt != null);
        }
        idx++;
        
        // ITEM_COMMENT
        if (get_comments) {
            vt = results[idx];
            assert(vt != null);
            for (Object row[] : vt) {
                long vals[] = {
                    (Long)row[0],
                    (Long)row[1],
                    (Long)row[2]
                };
                pending_commentResponse.add(vals);
            } // FOR
        }
        idx++;
      
        // ITEM Result Tables
        for ( ; idx < results.length; idx++) {
            vt = results[idx];
            assert(vt != null);
            for (Object row[] : vt) {
                ItemId itemId = this.processItemRecord(row);
                assert(itemId != null);
            } // FOR
        } // FOR
        
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NEW_BID
    // ----------------------------------------------------------------
    
    protected boolean executeNewBid(NewBid proc) throws SQLException {
        Date benchmarkTimes[] = this.getTimestampParameterArray();
        ItemInfo itemInfo = null;
        UserId sellerId;
        UserId buyerId;
        double bid;
        double maxBid;
        
        boolean has_available = (profile.getAvailableItemsCount() > 0);
        boolean has_ending = (profile.getEndingSoonItemsCount() > 0);
        boolean has_waiting = (profile.getWaitForPurchaseItemsCount() > 0);
        boolean has_completed = (profile.getCompleteItemsCount() > 0); 
        
        // Some NEW_BIDs will be for items that have already ended.
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
            // 50% of NEW_BIDS will be for items that are ending soon
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
    // NEW_COMMENT
    // ----------------------------------------------------------------
    
    protected boolean executeNewComment(NewComment proc) throws SQLException {
        Date benchmarkTimes[] = this.getTimestampParameterArray();
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
        
        pending_commentResponse.add(new long[] {
                (Long)results[0],
                (Long)results[1],
                (Long)results[2]
        });
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NEW_COMMENT_RESPONSE
    // ----------------------------------------------------------------
    
    protected boolean executeNewCommentResponse(NewCommentResponse proc) throws SQLException {
        Date benchmarkTimes[] = this.getTimestampParameterArray();
        Collections.shuffle(pending_commentResponse, profile.rng);
        long row[] = pending_commentResponse.remove(0);
        assert(row != null);
        
        long commentId = row[0];
        ItemId itemId = new ItemId(row[1]);
        UserId sellerId = itemId.getSellerId();
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
    // NEW_FEEDBACK
    // ----------------------------------------------------------------
    
    protected boolean executeNewFeedback(NewFeedback proc) throws SQLException {
        Date benchmarkTimes[] = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomCompleteItem();
        UserId sellerId = itemInfo.getSellerId();
        UserId buyerId = profile.getRandomBuyerId(sellerId);
        long rating = (long) profile.rng.number(-1, 1);
        String feedback = profile.rng.astring(10, 80);
        
        proc.run(conn, benchmarkTimes, itemInfo.itemId.encode(),
                                       sellerId.encode(),
                                       buyerId.encode(),
                                       rating,
                                       feedback);
        conn.commit();
        
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NEW_ITEM
    // ----------------------------------------------------------------

    protected boolean executeNewItem(NewItem proc) throws SQLException {
        Date benchmarkTimes[] = this.getTimestampParameterArray();
        UserId sellerId = profile.getRandomSellerId(this.getId());
        ItemId itemId = this.getNextItemId(sellerId);

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

        Object results[] = proc.run(conn, benchmarkTimes, itemId.encode(), sellerId.encode(),
                                                          categoryId, name, description,
                                                          duration, initial_price, attributes,
                                                          gag_ids, gav_ids, images);
        conn.commit();
                       
        itemId = this.processItemRecord(results);
        assert(itemId != null);
        
        return (true);
    }

    // ----------------------------------------------------------------
    // NEW_PURCHASE
    // ----------------------------------------------------------------
    
    protected boolean executeNewPurchase(NewPurchase proc) throws SQLException {
        Date benchmarkTimes[] = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomWaitForPurchaseItem();
        UserId sellerId = itemInfo.getSellerId();
        double buyer_credit = 0d;
        
        // Whether the buyer will not have enough money
        if (itemInfo.hasCurrentPrice()) {
            if (profile.rng.number(1, 100) < AuctionMarkConstants.PROB_NEW_PURCHASE_NOT_ENOUGH_MONEY) {
                buyer_credit = -1 * itemInfo.getCurrentPrice();
            } else {
                buyer_credit = itemInfo.getCurrentPrice();
                profile.removeWaitForPurchaseItem(itemInfo);
            }
        }
        
        Object results[] = proc.run(conn, benchmarkTimes, itemInfo.itemId.encode(),
                                                          sellerId.encode(),
                                                          buyer_credit);
        conn.commit();
        
        ItemId itemId = this.processItemRecord(results);
        assert(itemId != null);
        
        return (true);
    }
    
    // ----------------------------------------------------------------
    // UPDATE_ITEM
    // ----------------------------------------------------------------
    
    protected boolean executeUpdateItem(UpdateItem proc) throws SQLException {
        Date benchmarkTimes[] = this.getTimestampParameterArray();
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
