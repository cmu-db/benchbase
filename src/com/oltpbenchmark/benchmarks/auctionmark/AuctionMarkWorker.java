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

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.api.Procedure.UserAbortException;

import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkWorker;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkLoader;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkProfile;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.*;
import com.oltpbenchmark.benchmarks.auctionmark.util.GlobalAttributeValueId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemInfo;
import com.oltpbenchmark.benchmarks.auctionmark.util.UserId;
import com.oltpbenchmark.util.CompositeId;
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
        /**
         * Generate the parameters array
         * Any elements that are CompositeIds will automatically be encoded before being
         * shipped off to the H-Store cluster
         * @param client
         * @return
         */
        public Object[] generateParams(AuctionMarkWorker client);
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
            public Object[] generateParams(AuctionMarkWorker client) {
                return new Object[] { client.getTimestampParameterArray(),
                                      client.profile.getLastCloseAuctionsTime(),
                                      client.profile.updateAndGetLastCloseAuctionsTime() };
            }
            @Override
            public boolean canGenerateParam(AuctionMarkWorker client) {
                if (AuctionMarkConstants.ENABLE_CLOSE_AUCTIONS && client.getId() == 0) {
                    // If we've never checked before, then we'll want to do that now
                    if (client.profile.hasLastCloseAuctionsTime() == false) return (true);

                    // Otherwise check whether enough time has passed since the last time we checked
                    Date lastCheckWinningBidTime = client.profile.getLastCloseAuctionsTime();
                    Date currentTime = client.profile.getCurrentTime();
                    long time_elapsed = Math.round((currentTime.getTime() - lastCheckWinningBidTime.getTime()) / 1000.0);
                    if (LOG.isDebugEnabled()) LOG.debug(String.format("%s [start=%s, current=%s, elapsed=%d]", Transaction.CLOSE_AUCTIONS, client.profile.getBenchmarkStartTime(), currentTime, time_elapsed));
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
            public Object[] generateParams(AuctionMarkWorker client) {
                ItemInfo itemInfo = client.profile.getRandomAvailableItemId();
                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, itemInfo.getSellerId() };
            }

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
            public Object[] generateParams(AuctionMarkWorker client) {
                UserId userId = client.profile.getRandomBuyerId();
                int rand;
                
                // USER_FEEDBACK records
                rand = client.profile.rng.number(0, 100);
                boolean get_feedback = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_FEEDBACK); 

                // ITEM_COMMENT records
                rand = client.profile.rng.number(0, 100);
                boolean get_comments = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_COMMENTS);
                
                // Seller ITEM records
                rand = client.profile.rng.number(0, 100);
                boolean get_seller_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_SELLER_ITEMS); 

                // Buyer ITEM records
                rand = client.profile.rng.number(0, 100);
                boolean get_buyer_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_BUYER_ITEMS);
                
                // USER_WATCH records
                rand = client.profile.rng.number(0, 100);
                boolean get_watched_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_WATCHED_ITEMS); 
                
                return new Object[] { client.getTimestampParameterArray(),
                                      userId,
                                      get_feedback, get_comments,
                                      get_seller_items, get_buyer_items, get_watched_items };
            }
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
            public Object[] generateParams(AuctionMarkWorker client) {
                
                ItemInfo itemInfo = null;
                UserId sellerId;
                UserId buyerId;
                double bid;
                double maxBid;
                
                boolean has_available = (client.profile.getAvailableItemsCount() > 0);
                boolean has_ending = (client.profile.getEndingSoonItemsCount() > 0);
                boolean has_waiting = (client.profile.getWaitForPurchaseItemsCount() > 0);
                boolean has_completed = (client.profile.getCompleteItemsCount() > 0); 
                
                // Some NEW_BIDs will be for items that have already ended.
                // This will simulate somebody trying to bid at the very end but failing
                if ((has_waiting || has_completed) &&
                    (client.profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_NEWBID_CLOSED_ITEM || has_available == false)) {
                    if (has_waiting) {
                        itemInfo = client.profile.getRandomWaitForPurchaseItem();
                        assert(itemInfo != null) : "Failed to get WaitForPurchase itemInfo [" + client.profile.getWaitForPurchaseItemsCount() + "]";
                    } else {
                        itemInfo = client.profile.getRandomCompleteItem();
                        assert(itemInfo != null) : "Failed to get Completed itemInfo [" + client.profile.getCompleteItemsCount() + "]";
                    }
                    sellerId = itemInfo.getSellerId();
                    buyerId = client.profile.getRandomBuyerId(sellerId);
                    
                    // The bid/maxBid do not matter because they won't be able to actually
                    // update the auction
                    bid = client.profile.rng.nextDouble();
                    maxBid = bid + 100;
                }
                
                // Otherwise we want to generate information for a real bid
                else {
                    assert(has_available || has_ending);
                    // 50% of NEW_BIDS will be for items that are ending soon
                    if ((has_ending && client.profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_NEWBID_CLOSED_ITEM) || has_available == false) {
                        itemInfo = client.profile.getRandomEndingSoonItem(true);
                    }
                    if (itemInfo == null) {
                        itemInfo = client.profile.getRandomAvailableItem(true);
                    }
                    if (itemInfo == null) {
                        itemInfo = client.profile.getRandomItem();
                    }
                    
                    sellerId = itemInfo.getSellerId();
                    buyerId = client.profile.getRandomBuyerId(sellerId);
                    
                    double currentPrice = itemInfo.getCurrentPrice();
                    bid = client.profile.rng.fixedPoint(2, currentPrice, currentPrice * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP / 2)));
                    maxBid = client.profile.rng.fixedPoint(2, bid, (bid * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP / 2))));
                }

                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, sellerId, buyerId, maxBid, itemInfo.endDate };
            }
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
            public Object[] generateParams(AuctionMarkWorker client) {
                ItemInfo itemInfo = client.profile.getRandomCompleteItem();
                UserId sellerId = itemInfo.getSellerId();
                UserId buyerId = client.profile.getRandomBuyerId(sellerId);
                String question = client.profile.rng.astring(10, 128);
                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, sellerId, buyerId, question };
            }
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
            public Object[] generateParams(AuctionMarkWorker client) {
                Collections.shuffle(client.pending_commentResponse, client.profile.rng);
                long row[] = client.pending_commentResponse.remove(0);
                assert(row != null);
                
                long commentId = row[0];
                ItemId itemId = new ItemId(row[1]);
                UserId sellerId = itemId.getSellerId();
                String response = client.profile.rng.astring(10, 128);

                return new Object[] { client.getTimestampParameterArray(),
                                      itemId, sellerId, commentId, response };
            }
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
            public Object[] generateParams(AuctionMarkWorker client) {
                ItemInfo itemInfo = client.profile.getRandomCompleteItem();
                UserId sellerId = itemInfo.getSellerId();
                UserId buyerId = client.profile.getRandomBuyerId(sellerId);
                long rating = (long) client.profile.rng.number(-1, 1);
                String feedback = client.profile.rng.astring(10, 80);
                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, sellerId, buyerId, rating, feedback };
            }
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
            public Object[] generateParams(AuctionMarkWorker client) {
                UserId sellerId = client.profile.getRandomSellerId(client.getId());
                ItemId itemId = client.getNextItemId(sellerId);

                String name = client.profile.rng.astring(6, 32);
                String description = client.profile.rng.astring(50, 255);
                long categoryId = client.profile.getRandomCategoryId();

                Double initial_price = (double) client.profile.randomInitialPrice.nextInt();
                String attributes = client.profile.rng.astring(50, 255);

                int numAttributes = client.profile.randomNumAttributes.nextInt();
                List<GlobalAttributeValueId> gavList = new ArrayList<GlobalAttributeValueId>(numAttributes);
                for (int i = 0; i < numAttributes; i++) {
                    GlobalAttributeValueId gav_id = client.profile.getRandomGlobalAttributeValue();
                    if (!gavList.contains(gav_id)) gavList.add(gav_id);
                } // FOR

                long[] gag_ids = new long[gavList.size()];
                long[] gav_ids = new long[gavList.size()];
                for (int i = 0, cnt = gag_ids.length; i < cnt; i++) {
                    GlobalAttributeValueId gav_id = gavList.get(i);
                    gag_ids[i] = gav_id.getGlobalAttributeGroup().encode();
                    gav_ids[i] = gav_id.encode();
                } // FOR

                int numImages = client.profile.randomNumImages.nextInt();
                String[] images = new String[numImages];
                for (int i = 0; i < numImages; i++) {
                    images[i] = client.profile.rng.astring(20, 100);
                } // FOR

                long duration = client.profile.randomDuration.nextInt();

                return new Object[] { client.getTimestampParameterArray(),
                                      itemId, sellerId, categoryId,
                                      name, description, duration, initial_price, attributes,
                                      gag_ids, gav_ids, images };
            }
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
            public Object[] generateParams(AuctionMarkWorker client) {
                ItemInfo itemInfo = client.profile.getRandomWaitForPurchaseItem();
                UserId sellerId = itemInfo.getSellerId();
                double buyer_credit = 0d;
                
                // Whether the buyer will not have enough money
                if (itemInfo.hasCurrentPrice()) {
                    if (client.profile.rng.number(1, 100) < AuctionMarkConstants.PROB_NEW_PURCHASE_NOT_ENOUGH_MONEY) {
                        buyer_credit = -1 * itemInfo.getCurrentPrice();
                    } else {
                        buyer_credit = itemInfo.getCurrentPrice();
                        client.profile.removeWaitForPurchaseItem(itemInfo);
                    }
                }
                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, sellerId, buyer_credit };
            }
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
            public Object[] generateParams(AuctionMarkWorker client) {
                ItemInfo itemInfo = client.profile.getRandomAvailableItemId();
                UserId sellerId = itemInfo.getSellerId();
                String description = client.profile.rng.astring(50, 255);
                
                boolean delete_attribute = false;
                long add_attribute[] = {
                    -1,
                    -1
                };
                
                // Delete ITEM_ATTRIBUTE
                if (client.profile.rng.number(1, 100) < AuctionMarkConstants.PROB_UPDATEITEM_DELETE_ATTRIBUTE) {
                    delete_attribute = true;
                }
                // Add ITEM_ATTRIBUTE
                else if (client.profile.rng.number(1, 100) < AuctionMarkConstants.PROB_UPDATEITEM_ADD_ATTRIBUTE) {
                    GlobalAttributeValueId gav_id = client.profile.getRandomGlobalAttributeValue();
                    assert(gav_id != null);
                    add_attribute[0] = gav_id.getGlobalAttributeGroup().encode();
                    add_attribute[1] = gav_id.encode();
                }
                
                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, sellerId, description,
                                      delete_attribute, add_attribute };
            }
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
        
        /**
         * Given a BenchmarkProfile object, call the AuctionMarkParamGenerator object for a given
         * transaction type to generate a set of parameters for a new txn invocation 
         * @param profile
         * @return
         */
        public Object[] generateParams(AuctionMarkWorker client) {
            Object vals[] = this.generator.generateParams(client);
            // Automatically encode any CompositeIds
            for (int i = 0; i < vals.length; i++) {
                if (vals[i] instanceof CompositeId) vals[i] = ((CompositeId)vals[i]).encode();
            } // FOR
            return (vals);
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
    protected TransactionType doWork(boolean measure, Phase phase) {
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
        
        TransactionType next = null;
        
        // Always check if we need to want to run CLOSE_AUCTIONS
        // We only do this from the first client
        if (Transaction.CLOSE_AUCTIONS.canExecute(this)) {
            next = transactionTypes.getType(Transaction.CLOSE_AUCTIONS.procClass);
            assert(next != null);
        }
        // Find the next txn that we can run.
        // Example: NewBid can only be executed if there are item_ids retrieved by an earlier call by GetItem
        else {
            int safety = 1000;
            while (safety-- > 0) {
                TransactionType tempTxn = transactionTypes.getType(phase.chooseTransaction());
                Transaction txn = Transaction.get(tempTxn.getProcedureClass());
                if (txn.canExecute(this)) {
                    next = tempTxn;
                    break;
                }
            } // WHILE
            assert(next != null);
        }
        
        this.executeWork(next);
        return (next);
    }
    
    @Override
    protected void executeWork(TransactionType txnType) {
        Transaction txn = Transaction.get(txnType.getProcedureClass());
        Object[] params = txn.generateParams(this);
        
        if (params == null) {
            throw new RuntimeException("Unable to execute " + txn + " because the parameters were null?");
        } else if (LOG.isDebugEnabled()) {
            LOG.info("Executing new invocation of transaction " + txn);
        }
        
        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        boolean ret = false;
        try {
            try {
                switch (txn) {
//                    case CLOSE_AUCTIONS:
//                        ret = executeCloseAuctions((CloseAuctions)proc);
//                        break;
//                    case GET_ITEM:
//                        callback = new GetItemCallback(params);
//                        break;
//                    case GET_USER_INFO:
//                        callback = new GetUserInfoCallback(params);
//                        break;
//                    case NEW_COMMENT:
//                        callback = new NewCommentCallback(params);
//                        break;
//                    case NEW_ITEM:
//                        callback = new NewItemCallback(params);
//                        break;
//                    case NEW_PURCHASE:
//                        callback = new NewPurchaseCallback(params);
//                        break;
                    default:
                        assert(false) : "Unexpected transaction: " + txn; 
                } // SWITCH
                this.conn.commit();
            } catch (UserAbortException ex) {
                if (LOG.isDebugEnabled()) LOG.debug(proc + " Aborted", ex);
                this.conn.rollback();
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Unexpected error when executing " + proc, ex);
        }
        if (ret && LOG.isDebugEnabled()) LOG.debug("Executed a new invocation of " + txn);

        return;
    }
    
    /**********************************************************************************************
     * Base Callback
     **********************************************************************************************/
//    protected abstract class BaseCallback implements ProcedureCallback {
//        final Transaction txn;
//        final Object params[];
//        final Histogram<ItemStatus> updated = new Histogram<ItemStatus>();
//        
//        public BaseCallback(Transaction txn, Object params[]) {
//            this.txn = txn;
//            this.params = params;
//        }
//        
//        public abstract void process(VoltTable results[]);
//        
//        @Override
//        public void clientCallback(ClientResponse clientResponse) {
//            if (LOG.isTraceEnabled()) LOG.trace("clientCallback(cid = " + getId() + "):: txn = " + txn.getDisplayName());
//            incrementTransactionCounter(clientResponse, this.txn.ordinal());
//            VoltTable[] results = clientResponse.getResults();
//            if (clientResponse.getStatus() == Hstore.Status.OK) {
//                try {
//                    this.process(results);
//                } catch (Throwable ex) {
//                    LOG.error("PARAMS: " + Arrays.toString(this.params));
//                    for (int i = 0; i < results.length; i++) {
//                        LOG.info(String.format("[%02d] RESULT\n%s", i, results[i]));
//                    } // FOR
//                    throw new RuntimeException("Failed to process results for " + this.txn, ex);
//                }
//                    
//            } else {
//                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s: %s", this.txn, clientResponse.getStatusString()), clientResponse.getException());
//            }
//        }
//        /**
//         * For the given VoltTable that contains ITEM records, process the current
//         * row of that table and update the benchmark profile based on item information
//         * stored in that row. 
//         * @param vt
//         * @return
//         */
//        public ItemId processItemRecord(VoltTable vt) {
//            ItemId itemId = new ItemId(vt.getLong("i_id"));
//            Date endDate = vt.getTimestampAsTimestamp("i_end_date");
//            short numBids = (short)vt.getLong("i_num_bids");
//            double currentPrice = vt.getDouble("i_current_price");
//            ItemInfo itemInfo = new ItemInfo(itemId, currentPrice, endDate, numBids);
//            if (vt.hasColumn("ip_id")) itemInfo.status = ItemStatus.CLOSED;
//            if (vt.hasColumn("i_status")) itemInfo.status = ItemStatus.get(vt.getLong("i_status"));
//            
//            UserId sellerId = new UserId(vt.getLong("i_u_id"));
//            assert (itemId.getSellerId().equals(sellerId));
//            
//            ItemStatus qtype = profile.addItemToProperQueue(itemInfo, false);
//            this.updated.put(qtype);
//
//            return (itemId);
//        }
//        @Override
//        public String toString() {
//            String cnts[] = new String[ItemStatus.values().length];
//            for (ItemStatus qtype : ItemStatus.values()) {
//                cnts[qtype.ordinal()] = String.format("%s=+%d", qtype, updated.get(qtype, 0));
//            }
//            return String.format("%s :: %s", this.txn, StringUtil.join(", ", cnts));
//        }
//    } // END CLASS
//    
//    /**********************************************************************************************
//     * NULL Callback
//     **********************************************************************************************/
//    protected class NullCallback extends BaseCallback {
//        public NullCallback(Transaction txn, Object params[]) {
//            super(txn, params);
//        }
//        @Override
//        public void process(VoltTable[] results) {
//            // Nothing to do...
//        }
//    } // END CLASS
//    
//    /**********************************************************************************************
//     * CLOSE_AUCTIONS Callback
//     **********************************************************************************************/
//    protected class CloseAuctionsCallback extends BaseCallback {
//        public CloseAuctionsCallback(Object params[]) {
//            super(Transaction.CLOSE_AUCTIONS, params);
//        }
//        @Override
//        public void process(VoltTable[] results) {
//            assert (null != results && results.length > 0);
//            while (results[0].advanceRow()) {
//                ItemId itemId = this.processItemRecord(results[0]);
//                assert(itemId != null);
//            } // WHILE
//            if (LOG.isDebugEnabled()) LOG.debug(super.toString());
//            profile.updateItemQueues();
//        }
//    } // END CLASS
//    
//    /**********************************************************************************************
//     * NEW_COMMENT Callback
//     **********************************************************************************************/
//    protected class NewCommentCallback extends BaseCallback {
//        public NewCommentCallback(Object params[]) {
//            super(Transaction.NEW_COMMENT, params);
//        }
//        @Override
//        public void process(VoltTable[] results) {
//            assert(results.length == 1);
//            while (results[0].advanceRow()) {
//                long vals[] = {
//                    results[0].getLong("ic_id"),
//                    results[0].getLong("ic_i_id"),
//                    results[0].getLong("ic_u_id")
//                };
//                pending_commentResponse.add(vals);
//            } // WHILE
//        }
//    } // END CLASS
//        
//    /**********************************************************************************************
//     * GET_ITEM Callback
//     **********************************************************************************************/
//    protected class GetItemCallback extends BaseCallback {
//        public GetItemCallback(Object params[]) {
//            super(Transaction.GET_ITEM, params);
//        }
//        @Override
//        public void process(VoltTable[] results) {
//            assert (null != results && results.length > 0);
//            while (results[0].advanceRow()) {
//                ItemId itemId = this.processItemRecord(results[0]);
//                assert(itemId != null);
//            } // WHILE
//            if (LOG.isDebugEnabled()) LOG.debug(super.toString());
//        }
//    } // END CLASS
//    
//    /**********************************************************************************************
//     * GET_USER_INFO Callback
//     **********************************************************************************************/
//    protected class GetUserInfoCallback extends BaseCallback {
//        final boolean expect_user;
//        final boolean expect_feedback;
//        final boolean expect_comments;
//        final boolean expect_seller;
//        final boolean expect_buyer;
//        final boolean expect_watched;
//        
//        public GetUserInfoCallback(Object params[]) {
//            super(Transaction.GET_USER_INFO, params);
//            
//            int idx = 2;
//            this.expect_user     = true;
//            this.expect_feedback = (Boolean)params[idx++];
//            this.expect_comments = (Boolean)params[idx++];
//            this.expect_seller   = (Boolean)params[idx++];
//            this.expect_buyer    = (Boolean)params[idx++];
//            this.expect_watched  = (Boolean)params[idx++];
//        }
//        @Override
//        public void process(VoltTable[] results) {
//            int idx = 0;
//            
//            // USER
//            if (expect_user) {
//                VoltTable vt = results[idx++];
//                assert(vt != null);
//                assert(vt.getRowCount() > 0);
//            }
//            // USER_FEEDBACK
//            if (expect_feedback) {
//                VoltTable vt = results[idx++];
//                assert(vt != null);
//            }
//            // ITEM_COMMENT
//            if (expect_comments) {
//                VoltTable vt = results[idx++];
//                assert(vt != null);
//                while (vt.advanceRow()) {
//                    long vals[] = {
//                        vt.getLong("ic_id"),
//                        vt.getLong("ic_i_id"),
//                        vt.getLong("ic_u_id")
//                    };
//                    pending_commentResponse.add(vals);
//                } // WHILE
//            }
//            
//            // ITEM Result Tables
//            for (int i = idx; i < results.length; i++) {
//                VoltTable vt = results[i];
//                assert(vt != null);
//                while (vt.advanceRow()) {
//                    ItemId itemId = this.processItemRecord(vt);
//                    assert(itemId != null);
//                } // WHILE
//            } // FOR
//        }
//    } // END CLASS
//    
//    /**********************************************************************************************
//     * NEW_BID Callback
//     **********************************************************************************************/
//    protected class NewBidCallback extends BaseCallback {
//        public NewBidCallback(Object params[]) {
//            super(Transaction.NEW_BID, params);
//        }
//        @Override
//        public void process(VoltTable[] results) {
//            assert(results.length == 1);
//            while (results[0].advanceRow()) {
//                ItemId itemId = this.processItemRecord(results[0]);
//                assert(itemId != null);
//            } // WHILE
//            if (LOG.isDebugEnabled()) LOG.debug(super.toString());
//        }
//    } // END CLASS
//
//    /**********************************************************************************************
//     * NEW_ITEM Callback
//     **********************************************************************************************/
//    protected class NewItemCallback extends BaseCallback {
//        public NewItemCallback(Object params[]) {
//            super(Transaction.NEW_ITEM, params);
//        }
//        @Override
//        public void process(VoltTable[] results) {
//            assert(results.length == 1);
//            while (results[0].advanceRow()) {
//                ItemId itemId = this.processItemRecord(results[0]);
//                assert(itemId != null);
//            } // WHILE
//            if (LOG.isDebugEnabled()) LOG.debug(super.toString());
//        }
//    } // END CLASS
//    
//    /**********************************************************************************************
//     * NEW_PURCHASE Callback
//     **********************************************************************************************/
//    protected class NewPurchaseCallback extends BaseCallback {
//        public NewPurchaseCallback(Object params[]) {
//            super(Transaction.NEW_PURCHASE, params);
//        }
//        @Override
//        public void process(VoltTable[] results) {
//            assert(results.length == 1);
//            while (results[0].advanceRow()) {
//                ItemId itemId = this.processItemRecord(results[0]);
//                assert(itemId != null);
//            } // WHILE
//            if (LOG.isDebugEnabled()) LOG.debug(super.toString());
//        }
//    } // END CLASS
  
    
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
}
