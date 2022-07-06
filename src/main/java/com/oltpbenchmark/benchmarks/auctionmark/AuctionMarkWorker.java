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


package com.oltpbenchmark.benchmarks.auctionmark;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.auctionmark.exceptions.DuplicateItemIdException;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.*;
import com.oltpbenchmark.benchmarks.auctionmark.util.*;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.SQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class AuctionMarkWorker extends Worker<AuctionMarkBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(AuctionMarkWorker.class);

    // -----------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // -----------------------------------------------------------------

    protected final AuctionMarkProfile profile;

    private final AtomicBoolean closeAuctions_flag = new AtomicBoolean();

    private final Thread closeAuctions_checker;

    protected static final Map<String, Integer> ip_id_cntrs = new HashMap<>();

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


            Procedure proc = AuctionMarkWorker.this.getProcedure(txnType);


            long sleepTime = AuctionMarkConstants.CLOSE_AUCTIONS_INTERVAL / AuctionMarkConstants.TIME_SCALE_FACTOR;
            while (true) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Sleeping for %d seconds", sleepTime));
                }

                // Always sleep until the next time that we need to check
                try {
                    Thread.sleep(sleepTime * AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }

                if (AuctionMarkConstants.CLOSE_AUCTIONS_SEPARATE_THREAD) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Executing %s in separate thread", txnType));
                    }
                    try (Connection conn = getBenchmark().makeConnection()) {
                        executeCloseAuctions(conn, (CloseAuctions) proc);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    AuctionMarkWorker.this.closeAuctions_flag.set(true);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Marked ready flag for %s", txnType));
                    }
                }
            }
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
         *
         * @param client
         * @return
         */
        boolean canGenerateParam(AuctionMarkWorker client);
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
                return (!client.profile.pending_commentResponses.isEmpty());
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
         *
         * @param procClass
         * @param generator
         */
        Transaction(Class<? extends Procedure> procClass, AuctionMarkParamGenerator generator) {
            this.procClass = procClass;
            this.generator = generator;
        }

        public final Class<? extends Procedure> procClass;
        public final AuctionMarkParamGenerator generator;

        protected static final Map<Class<? extends Procedure>, Transaction> class_lookup = new HashMap<>();
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<>();

        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name(), vt);
                Transaction.class_lookup.put(vt.procClass, vt);
            }
        }

        public static Transaction get(Class<? extends Procedure> clazz) {
            return (Transaction.class_lookup.get(clazz));
        }

        /**
         * This will return true if we can call a new transaction for this procedure
         * A txn can be called if we can generate all of the parameters we need
         *
         * @return
         */
        public boolean canExecute(AuctionMarkWorker client) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Checking whether we can execute {} now", this);
            }
            return this.generator.canGenerateParam(client);
        }
    }

    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    /**
     * Constructor
     *
     * @param id
     * @param benchmark
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
        if (this.closeAuctions_checker != null) {
            this.closeAuctions_checker.start();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("AuctionMarkProfile %03d\n%s", this.getId(), profile));
        }
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) throws UserAbortException, SQLException {
        // We need to subtract the different between this and the profile's start time,
        // since that accounts for the time gap between when the loader started and when the client start.
        // Otherwise, all of our cache date will be out dated if it took a really long time
        // to load everything up. Again, in order to keep things in synch, we only want to
        // set this on the first call to runOnce(). This will account for start a bunch of
        // clients on multiple nodes but then having to wait until they're all up and running
        // before starting the actual benchmark run.
        if (!profile.hasClientStartTime()) {
            profile.setAndGetClientStartTime();
        }

        // Always update the current timestamp
        profile.updateAndGetCurrentTime();

        Transaction txn = null;

        // Always check if we need to want to run CLOSE_AUCTIONS
        // We only do this from the first client
        if (!AuctionMarkConstants.CLOSE_AUCTIONS_SEPARATE_THREAD &&
                closeAuctions_flag.compareAndSet(true, false)) {
            txn = Transaction.CloseAuctions;
            TransactionTypes txnTypes = this.getWorkloadConfiguration().getTransTypes();
            txnType = txnTypes.getType(txn.procClass);

        } else {
            txn = Transaction.get(txnType.getProcedureClass());
            if (!txn.canExecute(this)) {
                LOG.trace("Unable to execute {} because it is not ready.  Will RETRY_DIFFERENT.", txn);
                return (TransactionStatus.RETRY_DIFFERENT);
            }
        }

        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);

        if (LOG.isTraceEnabled()) {
            LOG.trace("{} -> {} -> {} -> {}", txnType, txn, txnType.getProcedureClass(), proc);
        }

        boolean ret = false;
        switch (txn) {
            case CloseAuctions:
                ret = executeCloseAuctions(conn, (CloseAuctions) proc);
                break;
            case GetItem:
                ret = executeGetItem(conn, (GetItem) proc);
                break;
            case GetUserInfo:
                ret = executeGetUserInfo(conn, (GetUserInfo) proc);
                break;
            case NewBid:
                ret = executeNewBid(conn, (NewBid) proc);
                break;
            case NewComment:
                ret = executeNewComment(conn, (NewComment) proc);
                break;
            case NewCommentResponse:
                ret = executeNewCommentResponse(conn, (NewCommentResponse) proc);
                break;
            case NewFeedback:
                ret = executeNewFeedback(conn, (NewFeedback) proc);
                break;
            case NewItem:
                ret = executeNewItem(conn, (NewItem) proc);
                break;
            case NewPurchase:
                ret = executeNewPurchase(conn, (NewPurchase) proc);
                break;
            case UpdateItem:
                ret = executeUpdateItem(conn, (UpdateItem) proc);
                break;
            default:

        }

        if (ret && LOG.isDebugEnabled()) {
            LOG.debug("Executed a new invocation of {}", txn);
        }

        return (TransactionStatus.SUCCESS);
    }

    /**
     * For the given VoltTable that contains ITEM records, process the current
     * row of that table and update the benchmark profile based on item information
     * stored in that row.
     *
     * @param row
     * @return
     * @see CloseAuctions
     * @see GetItem
     * @see GetUserInfo
     * @see NewBid
     * @see NewItem
     * @see NewPurchase
     */
    public ItemId processItemRecord(Object[] row) {
        int col = 0;
        ItemId i_id = new ItemId(SQLUtil.getString(row[col++]));  // i_id
        String i_u_id = SQLUtil.getString(row[col++]);              // i_u_id
        String i_name = (String) row[col++];                     // i_name
        double i_current_price = SQLUtil.getDouble(row[col++]); // i_current_price
        long i_num_bids = SQLUtil.getLong(row[col++]);          // i_num_bids
        Timestamp i_end_date = SQLUtil.getTimestamp(row[col++]);// i_end_date
        if (i_end_date == null) {
            LOG.warn("end date is null: {} / {} expected timestamp or date", row[col - 1], row[col - 1].getClass());
        }

        Long temp = SQLUtil.getLong(row[col++]);

        if (temp == null) {
            LOG.warn("status is null: {} / {} expected Long", row[col - 1], row[col - 1].getClass());

            ItemStatus i_status = ItemStatus.get(temp); // i_status

            ItemInfo itemInfo = new ItemInfo(i_id, i_current_price, i_end_date, (int) i_num_bids);
            itemInfo.setStatus(i_status);

            profile.addItemToProperQueue(itemInfo, false);
        }


        return (i_id);
    }

    public Timestamp[] getTimestampParameterArray() {
        return new Timestamp[]{profile.getLoaderStartTime(),
                profile.getClientStartTime()};
    }

    // ----------------------------------------------------------------
    // CLOSE_AUCTIONS
    // ----------------------------------------------------------------

    protected boolean executeCloseAuctions(Connection conn, CloseAuctions proc) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing {}", proc);
        }
        Timestamp[] benchmarkTimes = this.getTimestampParameterArray();
        Timestamp startTime = profile.getLastCloseAuctionsTime();
        Timestamp endTime = profile.updateAndGetLastCloseAuctionsTime();

        List<Object[]> results = proc.run(conn, benchmarkTimes, startTime, endTime);


        for (Object[] row : results) {
            ItemId itemId = this.processItemRecord(row);

        }
        profile.updateItemQueues();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished {}", proc);
        }
        return (true);
    }

    // ----------------------------------------------------------------
    // GetItem
    // ----------------------------------------------------------------

    protected boolean executeGetItem(Connection conn, GetItem proc) throws SQLException {
        Timestamp[] benchmarkTimes = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomAvailableItemId();

        Object[][] results = proc.run(conn, benchmarkTimes, itemInfo.getItemId().encode(),
                itemInfo.getSellerId().encode());

        // The first row will have our item data that we want
        // We don't care about the user information...
        ItemId itemId = this.processItemRecord(results[0]);


        return (true);
    }

    // ----------------------------------------------------------------
    // GetUserInfo
    // ----------------------------------------------------------------

    protected boolean executeGetUserInfo(Connection conn, GetUserInfo proc) throws SQLException {
        Timestamp[] benchmarkTimes = this.getTimestampParameterArray();
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

        UserInfo results = proc.run(conn, benchmarkTimes, userId.encode(),
                get_feedback,
                get_comments,
                get_seller_items,
                get_buyer_items,
                get_watched_items);


        // ITEM_COMMENTS
        if (get_comments) {

            for (Object[] row : results.getItemComments()) {
                String itemId = SQLUtil.getString(row[0]);
                String sellerId = SQLUtil.getString(row[1]);
                long commentId = SQLUtil.getLong(row[7]);

                ItemCommentResponse cr = new ItemCommentResponse(commentId, itemId, sellerId);
                profile.addPendingItemCommentResponse(cr);

            }
        }

        for (Object[] row : results.getSellerItems()) {
            ItemId itemId = this.processItemRecord(row);
        }

        for (Object[] row : results.getBuyerItems()) {
            ItemId itemId = this.processItemRecord(row);
        }

        for (Object[] row : results.getWatchedItems()) {
            ItemId itemId = this.processItemRecord(row);
        }


        return (true);
    }

    // ----------------------------------------------------------------
    // NewBid
    // ----------------------------------------------------------------

    protected boolean executeNewBid(Connection conn, NewBid proc) throws SQLException {
        Timestamp[] benchmarkTimes = this.getTimestampParameterArray();
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
                (profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_NEWBID_CLOSED_ITEM || !has_available)) {
            if (has_waiting) {
                itemInfo = profile.getRandomWaitForPurchaseItem();

            } else {
                itemInfo = profile.getRandomCompleteItem();

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

            // 50% of NewBids will be for items that are ending soon
            if ((has_ending && profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_NEWBID_CLOSED_ITEM) || !has_available) {
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

        Object[] results = proc.run(conn, benchmarkTimes, itemInfo.getItemId().encode(),
                sellerId.encode(),
                buyerId.encode(),
                maxBid,
                itemInfo.getEndDate());

        ItemId itemId = this.processItemRecord(results);


        return (true);
    }

    // ----------------------------------------------------------------
    // NewComment
    // ----------------------------------------------------------------

    protected boolean executeNewComment(Connection conn, NewComment proc) throws SQLException {
        Timestamp[] benchmarkTimes = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomCompleteItem();
        UserId sellerId = itemInfo.getSellerId();
        UserId buyerId = profile.getRandomBuyerId(sellerId);
        String question = profile.rng.astring(AuctionMarkConstants.ITEM_COMMENT_LENGTH_MIN,
                AuctionMarkConstants.ITEM_COMMENT_LENGTH_MAX);

        Object[] results = proc.run(conn, benchmarkTimes,
                itemInfo.getItemId().encode(),
                sellerId.encode(),
                buyerId.encode(),
                question);


        profile.pending_commentResponses.add(new ItemCommentResponse(SQLUtil.getLong(results[0]),
                SQLUtil.getString(results[1]),
                SQLUtil.getString(results[2])));
        return (true);
    }

    // ----------------------------------------------------------------
    // NewCommentResponse
    // ----------------------------------------------------------------

    protected boolean executeNewCommentResponse(Connection conn, NewCommentResponse proc) throws SQLException {
        Timestamp[] benchmarkTimes = this.getTimestampParameterArray();
        int idx = profile.rng.nextInt(profile.pending_commentResponses.size());
        ItemCommentResponse cr = profile.pending_commentResponses.remove(idx);


        long commentId = cr.getCommentId();
        ItemId itemId = new ItemId(cr.getItemId());
        UserId sellerId = itemId.getSellerId();

        String response = profile.rng.astring(AuctionMarkConstants.ITEM_COMMENT_LENGTH_MIN,
                AuctionMarkConstants.ITEM_COMMENT_LENGTH_MAX);

        proc.run(conn, benchmarkTimes, itemId.encode(),
                sellerId.encode(),
                commentId,
                response);

        return (true);
    }

    // ----------------------------------------------------------------
    // NewFeedback
    // ----------------------------------------------------------------

    protected boolean executeNewFeedback(Connection conn, NewFeedback proc) throws SQLException {
        Timestamp[] benchmarkTimes = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomCompleteItem();
        UserId sellerId = itemInfo.getSellerId();
        UserId buyerId = profile.getRandomBuyerId(sellerId);
        long rating = profile.rng.number(-1, 1);
        String feedback = profile.rng.astring(10, 80);

        String user_id;
        String from_id;
        if (profile.rng.nextBoolean()) {
            user_id = sellerId.encode();
            from_id = buyerId.encode();
        } else {
            user_id = buyerId.encode();
            from_id = sellerId.encode();
        }

        proc.run(conn, benchmarkTimes, user_id,
                itemInfo.getItemId().encode(),
                sellerId.encode(),
                from_id,
                rating,
                feedback);

        return (true);
    }

    // ----------------------------------------------------------------
    // NewItem
    // ----------------------------------------------------------------

    protected boolean executeNewItem(Connection conn, NewItem proc) throws SQLException {
        Timestamp[] benchmarkTimes = this.getTimestampParameterArray();
        UserId sellerId = profile.getRandomSellerId(this.getId());
        ItemId itemId = profile.getNextItemId(sellerId);

        String name = profile.rng.astring(6, 32);
        String description = profile.rng.astring(50, 255);
        long categoryId = profile.getRandomCategoryId();

        double initial_price = profile.randomInitialPrice.nextInt();
        String attributes = profile.rng.astring(50, 255);

        int numAttributes = profile.randomNumAttributes.nextInt();
        List<GlobalAttributeValueId> gavList = new ArrayList<>(numAttributes);
        for (int i = 0; i < numAttributes; i++) {
            GlobalAttributeValueId gav_id = profile.getRandomGlobalAttributeValue();
            if (!gavList.contains(gav_id)) {
                gavList.add(gav_id);
            }
        }

        String[] gag_ids = new String[gavList.size()];
        String[] gav_ids = new String[gavList.size()];
        for (int i = 0, cnt = gag_ids.length; i < cnt; i++) {
            GlobalAttributeValueId gav_id = gavList.get(i);
            gag_ids[i] = gav_id.getGlobalAttributeGroup().encode();
            gav_ids[i] = gav_id.encode();
        }

        int numImages = profile.randomNumImages.nextInt();
        String[] images = new String[numImages];
        for (int i = 0; i < numImages; i++) {
            images[i] = profile.rng.astring(20, 100);
        }

        long duration = profile.randomDuration.nextInt();

        Object[] results = null;
        try {
            String itemIdEncoded = itemId.encode();
            results = proc.run(conn, benchmarkTimes, itemIdEncoded, sellerId.encode(),
                    categoryId, name, description,
                    duration, initial_price, attributes,
                    gag_ids, gav_ids, images);
        } catch (DuplicateItemIdException ex) {
            profile.seller_item_cnt.set(sellerId, ex.getItemCount());
            LOG.warn("a duplicate item existed; i believe this error should be handled differently.: " + ex.getMessage());
            throw ex;
        }

        itemId = this.processItemRecord(results);


        return (true);
    }

    // ----------------------------------------------------------------
    // NewPurchase
    // ----------------------------------------------------------------

    protected boolean executeNewPurchase(Connection conn, NewPurchase proc) throws SQLException {
        Timestamp[] benchmarkTimes = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomWaitForPurchaseItem();
        String encodedItemId = itemInfo.getItemId().encode();
        UserId sellerId = itemInfo.getSellerId();
        double buyer_credit = 0d;

        Integer ip_id_cnt = ip_id_cntrs.get(encodedItemId);
        if (ip_id_cnt == null) {
            ip_id_cnt = 0;
        }

        String ip_id = AuctionMarkUtil.getUniqueElementId(encodedItemId,
                ip_id_cnt);
        ip_id_cntrs.put(encodedItemId, (ip_id_cnt < 127) ? ip_id_cnt + 1 : 0);

        // Whether the buyer will not have enough money
        if (itemInfo.hasCurrentPrice()) {
            if (profile.rng.number(1, 100) < AuctionMarkConstants.PROB_NEWPURCHASE_NOT_ENOUGH_MONEY) {
                buyer_credit = -1 * itemInfo.getCurrentPrice();
            } else {
                buyer_credit = itemInfo.getCurrentPrice();
                itemInfo.setStatus(ItemStatus.CLOSED);
            }
        }

        Object[] results = proc.run(conn, benchmarkTimes, encodedItemId,
                sellerId.encode(),
                ip_id,
                buyer_credit);

        ItemId itemId = this.processItemRecord(results);


        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateItem
    // ----------------------------------------------------------------

    protected boolean executeUpdateItem(Connection conn, UpdateItem proc) throws SQLException {
        Timestamp[] benchmarkTimes = this.getTimestampParameterArray();
        ItemInfo itemInfo = profile.getRandomAvailableItemId();
        UserId sellerId = itemInfo.getSellerId();
        String description = profile.rng.astring(50, 255);

        boolean delete_attribute = false;
        String[] add_attribute = {
                "-1",
                "-1"
        };

        // Delete ITEM_ATTRIBUTE
        if (profile.rng.number(1, 100) < AuctionMarkConstants.PROB_UPDATEITEM_DELETE_ATTRIBUTE) {
            delete_attribute = true;
        }
        // Add ITEM_ATTRIBUTE
        else if (profile.rng.number(1, 100) < AuctionMarkConstants.PROB_UPDATEITEM_ADD_ATTRIBUTE) {
            GlobalAttributeValueId gav_id = profile.getRandomGlobalAttributeValue();

            add_attribute[0] = gav_id.getGlobalAttributeGroup().encode();
            add_attribute[1] = gav_id.encode();
        }

        proc.run(conn, benchmarkTimes, itemInfo.getItemId().encode(),
                sellerId.encode(),
                description,
                delete_attribute,
                add_attribute);

        return (true);
    }
}
