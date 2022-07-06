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

import com.oltpbenchmark.benchmarks.auctionmark.procedures.Config;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.LoadConfig;
import com.oltpbenchmark.benchmarks.auctionmark.procedures.ResetDatabase;
import com.oltpbenchmark.benchmarks.auctionmark.util.*;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.*;
import com.oltpbenchmark.util.RandomDistribution.DiscreteRNG;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.RandomDistribution.Gaussian;
import com.oltpbenchmark.util.RandomDistribution.Zipf;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;


/**
 * AuctionMark Profile Information
 *
 * @author pavlo
 */
public class AuctionMarkProfile {
    private static final Logger LOG = LoggerFactory.getLogger(AuctionMarkProfile.class);

    /**
     * We maintain a cached version of the profile that we will copy from
     * This prevents the need to have every single client thread load up a separate copy
     */
    private static AuctionMarkProfile cachedProfile;

    // ----------------------------------------------------------------
    // REQUIRED REFERENCES
    // ----------------------------------------------------------------

    private final AuctionMarkBenchmark benchmark;

    private int client_id;

    /**
     * Specialized random number generator
     */
    protected transient final RandomGenerator rng;

    /**
     * The total number of clients in this benchmark invocation. Each
     * client will be responsible for adding new auctions for unique set of sellers
     * This may change per benchmark invocation.
     */
    private transient final int num_clients;

    // ----------------------------------------------------------------
    // SERIALIZABLE DATA MEMBERS
    // ----------------------------------------------------------------

    /**
     * Database Scale Factor
     */
    protected double scale_factor;

    /**
     * The start time used when creating the data for this benchmark
     */
    private Timestamp loaderStartTime;

    /**
     * The stop time for when the loader was finished
     * We can reset anything that has a timestamp after this one
     */
    private Timestamp loaderStopTime;

    /**
     * A histogram for the number of users that have the number of items listed
     * ItemCount -> # of Users
     */
    protected Histogram<Long> users_per_itemCount = new Histogram<>();

    // ----------------------------------------------------------------
    // TRANSIENT DATA MEMBERS
    // ----------------------------------------------------------------

    /**
     * Histogram for number of items per category (stored as category_id)
     */
    protected transient Histogram<Integer> items_per_category = new Histogram<>();

    /**
     * Three status types for an item:
     * (1) Available - The auction of this item is still open
     * (2) Ending Soon
     * (2) Wait for Purchase - The auction of this item is still open.
     * There is a bid winner and the bid winner has not purchased the item.
     * (3) Complete (The auction is closed and (There is no bid winner or
     * the bid winner has already purchased the item)
     */
    private transient final LinkedList<ItemInfo> items_available = new LinkedList<>();
    private transient final LinkedList<ItemInfo> items_endingSoon = new LinkedList<>();
    private transient final LinkedList<ItemInfo> items_waitingForPurchase = new LinkedList<>();
    private transient final LinkedList<ItemInfo> items_completed = new LinkedList<>();

    @SuppressWarnings("unchecked")
    protected transient final LinkedList<ItemInfo>[] allItemSets = new LinkedList[]{
            this.items_available,
            this.items_endingSoon,
            this.items_waitingForPurchase,
            this.items_completed,
    };

    /**
     * Internal list of GlobalAttributeGroupIds
     */
    protected transient List<GlobalAttributeGroupId> gag_ids = new ArrayList<>();

    /**
     * Internal map of UserIdGenerators
     */
    private transient UserIdGenerator userIdGenerator;

    /**
     * Random time different in seconds
     */
    public transient final DiscreteRNG randomTimeDiff;

    /**
     * Random duration in days
     */
    public transient final Gaussian randomDuration;

    protected transient final Zipf randomNumImages;
    protected transient final Zipf randomNumAttributes;
    protected transient final Zipf randomPurchaseDuration;
    protected transient final Zipf randomNumComments;
    protected transient final Zipf randomInitialPrice;

    private transient FlatHistogram<Integer> randomCategory;
    private transient FlatHistogram<Long> randomItemCount;

    /**
     * The last time that we called CHECK_WINNING_BIDS on this client
     */
    private transient final Timestamp lastCloseAuctionsTime = new Timestamp(0);
    /**
     * When this client started executing
     */
    private transient final Timestamp clientStartTime = new Timestamp(0);
    /**
     * Current Timestamp
     */
    private transient final Timestamp currentTime = new Timestamp(0);

    /**
     * TODO
     */
    protected transient final Histogram<UserId> seller_item_cnt = new Histogram<>();

    /**
     * TODO
     */
    protected transient final List<ItemCommentResponse> pending_commentResponses = new ArrayList<>();

    // -----------------------------------------------------------------
    // TEMPORARY VARIABLES
    // -----------------------------------------------------------------

    private transient final Set<ItemInfo> tmp_seenItems = new HashSet<>();
    private transient final Histogram<UserId> tmp_userIdHistogram = new Histogram<>(true);
    private transient final Timestamp tmp_now = new Timestamp(System.currentTimeMillis());

    // -----------------------------------------------------------------
    // CONSTRUCTOR
    // -----------------------------------------------------------------

    /**
     * Constructor - Keep your pimp hand strong!
     */
    public AuctionMarkProfile(AuctionMarkBenchmark benchmark, RandomGenerator rng) {
        this(benchmark, -1, rng);
    }

    private AuctionMarkProfile(AuctionMarkBenchmark benchmark, int client_id, RandomGenerator rng) {
        this.benchmark = benchmark;
        this.client_id = client_id;
        this.rng = rng;
        this.scale_factor = benchmark.getWorkloadConfiguration().getScaleFactor();
        this.num_clients = benchmark.getWorkloadConfiguration().getTerminals();
        this.loaderStartTime = new Timestamp(System.currentTimeMillis());

        this.randomInitialPrice = new Zipf(this.rng,
                AuctionMarkConstants.ITEM_INITIAL_PRICE_MIN,
                AuctionMarkConstants.ITEM_INITIAL_PRICE_MAX,
                AuctionMarkConstants.ITEM_INITIAL_PRICE_SIGMA);

        // Random time difference in a second scale
        this.randomTimeDiff = new Gaussian(this.rng,
                AuctionMarkConstants.ITEM_PRESERVE_DAYS * 24 * 60 * 60 * -1,
                AuctionMarkConstants.ITEM_DURATION_DAYS_MAX * 24 * 60 * 60);

        this.randomDuration = new Gaussian(this.rng,
                AuctionMarkConstants.ITEM_DURATION_DAYS_MIN,
                AuctionMarkConstants.ITEM_DURATION_DAYS_MAX);

        this.randomPurchaseDuration = new Zipf(this.rng,
                AuctionMarkConstants.ITEM_PURCHASE_DURATION_DAYS_MIN,
                AuctionMarkConstants.ITEM_PURCHASE_DURATION_DAYS_MAX,
                AuctionMarkConstants.ITEM_PURCHASE_DURATION_DAYS_SIGMA);

        this.randomNumImages = new Zipf(this.rng,
                AuctionMarkConstants.ITEM_NUM_IMAGES_MIN,
                AuctionMarkConstants.ITEM_NUM_IMAGES_MAX,
                AuctionMarkConstants.ITEM_NUM_IMAGES_SIGMA);

        this.randomNumAttributes = new Zipf(this.rng,
                AuctionMarkConstants.ITEM_NUM_GLOBAL_ATTRS_MIN,
                AuctionMarkConstants.ITEM_NUM_GLOBAL_ATTRS_MAX,
                AuctionMarkConstants.ITEM_NUM_GLOBAL_ATTRS_SIGMA);

        this.randomNumComments = new Zipf(this.rng,
                AuctionMarkConstants.ITEM_NUM_COMMENTS_MIN,
                AuctionMarkConstants.ITEM_NUM_COMMENTS_MAX,
                AuctionMarkConstants.ITEM_NUM_COMMENTS_SIGMA);

        if (LOG.isTraceEnabled()) {
            LOG.trace("AuctionMarkBenchmarkProfile :: constructor");
        }
    }

    // -----------------------------------------------------------------
    // SERIALIZATION METHODS
    // -----------------------------------------------------------------

    protected final void saveProfile(Connection conn) throws SQLException {
        this.loaderStopTime = new Timestamp(System.currentTimeMillis());

        // CONFIG_PROFILE
        Table catalog_tbl = this.benchmark.getCatalog().getTable(AuctionMarkConstants.TABLENAME_CONFIG_PROFILE);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.benchmark.getWorkloadConfiguration().getDatabaseType());
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int param_idx = 1;
            stmt.setObject(param_idx++, this.scale_factor); // CFP_SCALE_FACTOR
            stmt.setObject(param_idx++, this.loaderStartTime); // CFP_LOADER_START
            stmt.setObject(param_idx++, this.loaderStopTime); // CFP_LOADER_STOP
            stmt.setObject(param_idx++, this.users_per_itemCount.toJSONString()); // CFP_USER_ITEM_HISTOGRAM
            int result = stmt.executeUpdate();
            if (result != 1) {
                throw new RuntimeException("Bad update!");
            }
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("Saving profile information into {}", catalog_tbl);
        }
    }

    private AuctionMarkProfile copyProfile(AuctionMarkWorker worker, AuctionMarkProfile other) {
        this.client_id = worker.getId();
        this.scale_factor = other.scale_factor;
        this.loaderStartTime = other.loaderStartTime;
        this.loaderStopTime = other.loaderStopTime;
        this.users_per_itemCount = other.users_per_itemCount;
        this.items_per_category = other.items_per_category;
        this.gag_ids = other.gag_ids;

        // Initialize the UserIdGenerator so we can figure out whether our 
        // client should even have these ids
        this.initializeUserIdGenerator(this.client_id);


        for (int i = 0; i < this.allItemSets.length; i++) {
            LinkedList<ItemInfo> list = this.allItemSets[i];

            LinkedList<ItemInfo> origList = other.allItemSets[i];


            for (ItemInfo itemInfo : origList) {
                UserId sellerId = itemInfo.getSellerId();
                if (this.userIdGenerator.checkClient(sellerId)) {
                    this.seller_item_cnt.set(sellerId, sellerId.getItemCount());
                    list.add(itemInfo);
                }
            }
            Collections.shuffle(list);
        }

        for (ItemCommentResponse cr : other.pending_commentResponses) {
            UserId sellerId = new UserId(cr.getSellerId());
            if (this.userIdGenerator.checkClient(sellerId)) {
                this.pending_commentResponses.add(cr);
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("SellerItemCounts:\n{}", this.seller_item_cnt);
        }

        return (this);
    }

    protected static void clearCachedProfile() {
        cachedProfile = null;
    }

    /**
     * Load the profile information stored in the database
     *
     * @param
     */
    protected void loadProfile(AuctionMarkWorker worker) throws SQLException {
        synchronized (AuctionMarkProfile.class) {
            // Check whether we have a cached Profile we can copy from
            if (cachedProfile == null) {

                // Store everything in the cached profile.
                // We can then copy from that and extract out only the records
                // that we need for each AuctionMarkWorker
                cachedProfile = new AuctionMarkProfile(this.benchmark, this.rng);

                // Otherwise we have to go fetch everything again
                // So first we want to reset the database
                try (Connection conn = benchmark.makeConnection()) {

                    if (AuctionMarkConstants.RESET_DATABASE_ENABLE) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Resetting database from last execution run");
                        }
                        worker.getProcedure(ResetDatabase.class).run(conn);
                    }

                }


                // Then invoke LoadConfig to pull down the profile information we need
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Loading AuctionMarkProfile for the first time");
                }

                Config results;
                try (Connection conn = benchmark.makeConnection()) {
                    results = worker.getProcedure(LoadConfig.class).run(conn);
                }

                // CONFIG_PROFILE
                loadConfigProfile(cachedProfile, results.getConfigProfile());

                // IMPORTANT: We need to set these timestamps here. It must be done
                // after we have loaded benchmarkStartTime
                cachedProfile.setAndGetClientStartTime();
                cachedProfile.updateAndGetCurrentTime();

                // ITEM CATEGORY COUNTS
                loadItemCategoryCounts(cachedProfile, results.getCategoryCounts());

                // GLOBAL_ATTRIBUTE_GROUPS
                loadGlobalAttributeGroups(cachedProfile, results.getAttributes());

                // PENDING COMMENTS
                loadPendingItemComments(cachedProfile, results.getPendingComments());


                loadItems(cachedProfile, results.getOpenItems());
                loadItems(cachedProfile, results.getWaitingForPurchaseItems());
                loadItems(cachedProfile, results.getClosedItems());


                if (LOG.isDebugEnabled()) {
                    LOG.debug("Loaded profile:\n{}", cachedProfile.toString());
                }
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Using cached AuctionMarkProfile");
        }
        this.copyProfile(worker, cachedProfile);

    }

    private void initializeUserIdGenerator(int clientId) {


        this.userIdGenerator = new UserIdGenerator(this.users_per_itemCount,
                this.num_clients,
                (clientId < 0 ? null : clientId));
    }

    private static void loadConfigProfile(AuctionMarkProfile profile, List<Object[]> vt) {
        for (Object[] row : vt) {
            profile.scale_factor = SQLUtil.getDouble(row[0]);
            profile.loaderStartTime = SQLUtil.getTimestamp(row[1]);
            profile.loaderStopTime = SQLUtil.getTimestamp(row[2]);
            JSONUtil.fromJSONString(profile.users_per_itemCount, SQLUtil.getString(row[3]));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Loaded %s data", AuctionMarkConstants.TABLENAME_CONFIG_PROFILE));
        }
    }

    private static void loadItemCategoryCounts(AuctionMarkProfile profile, List<Object[]> vt) {
        for (Object[] row : vt) {
            long i_c_id = SQLUtil.getLong(row[0]);
            int count = SQLUtil.getInteger(row[1]);
            profile.items_per_category.put((int) i_c_id, count);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Loaded %d CATEGORY records from %s",
                    profile.items_per_category.getValueCount(), AuctionMarkConstants.TABLENAME_ITEM));
        }
    }

    private static void loadItems(AuctionMarkProfile profile, List<Object[]> vt) {
        int ctr = 0;
        for (Object[] row : vt) {
            ItemId i_id = new ItemId(SQLUtil.getString(row[0]));
            double i_current_price = SQLUtil.getDouble(row[1]);
            Timestamp i_end_date = SQLUtil.getTimestamp(row[2]);
            int i_num_bids = SQLUtil.getInteger(row[3]);

            // IMPORTANT: Do not set the status here so that we make sure that
            // it is added to the right queue
            ItemInfo itemInfo = new ItemInfo(i_id, i_current_price, i_end_date, i_num_bids);
            profile.addItemToProperQueue(itemInfo, false);
            ctr++;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Loaded %d records from %s",
                    ctr, AuctionMarkConstants.TABLENAME_ITEM));
        }
    }

    private static void loadPendingItemComments(AuctionMarkProfile profile, List<Object[]> vt) {
        for (Object[] row : vt) {
            int col = 1;
            long ic_id = SQLUtil.getLong(row[0]);
            String ic_i_id = SQLUtil.getString(row[1]);
            String ic_u_id = SQLUtil.getString(row[2]);
            ItemCommentResponse cr = new ItemCommentResponse(ic_id, ic_i_id, ic_u_id);
            profile.pending_commentResponses.add(cr);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Loaded %d records from %s",
                    profile.pending_commentResponses.size(), AuctionMarkConstants.TABLENAME_ITEM_COMMENT));
        }
    }

    private static void loadGlobalAttributeGroups(AuctionMarkProfile profile, List<Object[]> vt) {
        for (Object[] row : vt) {
            String gag_id = SQLUtil.getString(row[0]);
            profile.gag_ids.add(new GlobalAttributeGroupId(gag_id));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Loaded %d records from %s",
                    profile.gag_ids.size(), AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP));
        }
    }

    // -----------------------------------------------------------------
    // TIME METHODS
    // -----------------------------------------------------------------

    private Timestamp getScaledCurrentTimestamp(Timestamp time) {

        tmp_now.setTime(System.currentTimeMillis());
        time.setTime(AuctionMarkUtil.getScaledTimestamp(this.loaderStartTime, this.clientStartTime, tmp_now));
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Scaled:%d / Now:%d / BenchmarkStart:%d / ClientStart:%d",
                    time.getTime(), tmp_now.getTime(), this.loaderStartTime.getTime(), this.clientStartTime.getTime()));
        }
        return (time);
    }

    public synchronized Timestamp updateAndGetCurrentTime() {
        this.getScaledCurrentTimestamp(this.currentTime);
        if (LOG.isTraceEnabled()) {
            LOG.trace("CurrentTime: {}", currentTime);
        }
        return this.currentTime;
    }

    public Timestamp getCurrentTime() {
        return this.currentTime;
    }

    public Timestamp getLoaderStartTime() {
        return (this.loaderStartTime);
    }

    public Timestamp getLoaderStopTime() {
        return (this.loaderStopTime);
    }

    public Timestamp setAndGetClientStartTime() {

        this.clientStartTime.setTime(System.currentTimeMillis());
        return (this.clientStartTime);
    }

    public Timestamp getClientStartTime() {
        return (this.clientStartTime);
    }

    public boolean hasClientStartTime() {
        return (this.clientStartTime.getTime() != 0);
    }

    public synchronized Timestamp updateAndGetLastCloseAuctionsTime() {
        this.getScaledCurrentTimestamp(this.lastCloseAuctionsTime);
        return this.lastCloseAuctionsTime;
    }

    public Timestamp getLastCloseAuctionsTime() {
        return this.lastCloseAuctionsTime;
    }


    // -----------------------------------------------------------------
    // GENERAL METHODS
    // -----------------------------------------------------------------

    /**
     * Get the scale factor value for this benchmark profile
     *
     * @return
     */
    public double getScaleFactor() {
        return (this.scale_factor);
    }

    /**
     * Set the scale factor for this benchmark profile
     *
     * @param scale_factor
     */
    public void setScaleFactor(double scale_factor) {

        this.scale_factor = scale_factor;
    }

    // ----------------------------------------------------------------
    // USER METHODS
    // ----------------------------------------------------------------

    /**
     * Note that this synchronization block only matters for the loader
     *
     * @param min_item_count
     * @param clientId       - Will use null if less than zero
     * @param exclude
     * @return
     */
    private synchronized UserId getRandomUserId(int min_item_count, int clientId, UserId... exclude) {
        // We use the UserIdGenerator to ensure that we always select the next UserId for
        // a given client from the same set of UserIds
        if (this.randomItemCount == null) {
            this.randomItemCount = new FlatHistogram<>(this.rng, this.users_per_itemCount);
        }
        if (this.userIdGenerator == null) {
            this.initializeUserIdGenerator(clientId);
        }

        UserId user_id = null;
        int tries = 1000;
        final long num_users = this.userIdGenerator.getTotalUsers() - 1;
        while (user_id == null && tries-- > 0) {
            // We first need to figure out how many items our seller needs to have
            long itemCount = -1;
            // assert(min_item_count < this.users_per_item_count.getMaxValue());
            while (itemCount < min_item_count) {
                itemCount = this.randomItemCount.nextValue();
            }

            // Set the current item count and then choose a random position
            // between where the generator is currently at and where it ends
            this.userIdGenerator.setCurrentItemCount((int) itemCount);
            long cur_position = this.userIdGenerator.getCurrentPosition();
            long new_position = rng.number(cur_position, num_users);
            user_id = this.userIdGenerator.seekToPosition((int) new_position);
            if (user_id == null) {
                continue;
            }

            // Make sure that we didn't select the same UserId as the one we were
            // told to exclude.
            if (exclude != null && exclude.length > 0) {
                for (UserId ex : exclude) {
                    if (ex != null && ex.equals(user_id)) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Excluding {}", user_id);
                        }
                        user_id = null;
                        break;
                    }
                }
                if (user_id == null) {
                    continue;
                }
            }

            // If we don't care about skew, then we're done right here
            if (LOG.isTraceEnabled()) {
                LOG.trace("Selected {}", user_id);
            }
            break;
        }
        if (user_id == null && LOG.isDebugEnabled()) {
            LOG.warn(String.format("Failed to select a random UserId " +
                            "[minItemCount=%d, clientId=%d, exclude=%s, totalPossible=%d, currentPosition=%d]",
                    min_item_count, clientId, Arrays.toString(exclude),
                    this.userIdGenerator.getTotalUsers(), this.userIdGenerator.getCurrentPosition()));
        }
        return (user_id);
    }

    /**
     * Gets a random buyer ID for all clients
     *
     * @return
     */
    public UserId getRandomBuyerId(UserId... exclude) {
        // We don't care about skewing the buyerIds at this point, so just get one from getRandomUserId
        return (this.getRandomUserId(0, -1, exclude));
    }

    /**
     * Get a random buyer UserId, where the probability that a particular user is selected
     * increases based on the number of bids that they have made in the past. We won't allow
     * the last bidder to be selected again
     *
     * @param previousBidders
     * @return
     */
    public UserId getRandomBuyerId(Histogram<UserId> previousBidders, UserId... exclude) {
        // This is very inefficient, but it's probably good enough for now
        tmp_userIdHistogram.clear();
        tmp_userIdHistogram.putHistogram(previousBidders);
        for (UserId ex : exclude) {
            tmp_userIdHistogram.removeAll(ex);
        }
        tmp_userIdHistogram.put(this.getRandomBuyerId(exclude));


        LOG.trace("New Histogram:\n{}", tmp_userIdHistogram);

        FlatHistogram<UserId> rand_h = new FlatHistogram<>(rng, tmp_userIdHistogram);
        return (rand_h.nextValue());
    }

    /**
     * Gets a random SellerID for the given client
     *
     * @return
     */
    public UserId getRandomSellerId(int client) {
        return (this.getRandomUserId(1, client));
    }

    public void addPendingItemCommentResponse(ItemCommentResponse cr) {
        if (this.client_id != -1) {
            UserId sellerId = new UserId(cr.getSellerId());
            if (!this.userIdGenerator.checkClient(sellerId)) {
                return;
            }
        }
        this.pending_commentResponses.add(cr);
    }

    // ----------------------------------------------------------------
    // ITEM METHODS
    // ----------------------------------------------------------------


    public ItemId getNextItemId(UserId seller_id) {
        Integer cnt = this.seller_item_cnt.get(seller_id);
        if (cnt == null || cnt == 0) {
            cnt = seller_id.getItemCount();
            //this.seller_item_cnt.put(seller_id, cnt);
        }
        this.seller_item_cnt.put(seller_id, cnt);
        return (new ItemId(seller_id, cnt));
    }

    private boolean addItem(LinkedList<ItemInfo> items, ItemInfo itemInfo) {
        boolean added = false;

        int idx = items.indexOf(itemInfo);
        if (idx != -1) {
            // HACK: Always swap existing ItemInfos with our new one, since it will
            // more up-to-date information
            ItemInfo existing = items.set(idx, itemInfo);

            return (true);
        }
        if (itemInfo.hasCurrentPrice())


        // If we have room, shove it right in
        // We'll throw it in the back because we know it hasn't been used yet
        {
            if (items.size() < AuctionMarkConstants.ITEM_ID_CACHE_SIZE) {
                items.addLast(itemInfo);
                added = true;

                // Otherwise, we can will randomly decide whether to pop one out
            } else if (this.rng.nextBoolean()) {
                items.pop();
                items.addLast(itemInfo);
                added = true;
            }
        }
        return (added);
    }

    public void updateItemQueues() {
        Timestamp currentTime = this.updateAndGetCurrentTime();


        for (LinkedList<ItemInfo> items : allItemSets) {
            // If the items is already in the completed queue, then we don't need
            // to do anything with it.
            if (items == this.items_completed) {
                continue;
            }

            for (ItemInfo itemInfo : items) {
                this.addItemToProperQueue(itemInfo, currentTime);
            }
        }

        if (LOG.isDebugEnabled()) {
            Map<ItemStatus, Integer> m = new HashMap<>();
            m.put(ItemStatus.OPEN, this.items_available.size());
            m.put(ItemStatus.ENDING_SOON, this.items_endingSoon.size());
            m.put(ItemStatus.WAITING_FOR_PURCHASE, this.items_waitingForPurchase.size());
            m.put(ItemStatus.CLOSED, this.items_completed.size());
            LOG.debug(String.format("Updated Item Queues [%s]:\n%s",
                    currentTime, StringUtil.formatMaps(m)));
        }
    }

    public ItemStatus addItemToProperQueue(ItemInfo itemInfo, boolean is_loader) {
        // Calculate how much time is left for this auction
        Timestamp baseTime = (is_loader ? this.getLoaderStartTime() : this.getCurrentTime());


        return addItemToProperQueue(itemInfo, baseTime);
    }

    private ItemStatus addItemToProperQueue(ItemInfo itemInfo, Timestamp baseTime) {
        // Always check whether we even want it for this client
        // The loader's profile and the cache profile will always have a negative client_id,
        // which means that we always want to keep it
        if (this.client_id != -1) {
            if (this.userIdGenerator == null) {
                this.initializeUserIdGenerator(this.client_id);
            }
            if (!this.userIdGenerator.checkClient(itemInfo.getSellerId())) {
                return (null);
            }
        }

        long remaining = itemInfo.getEndDate().getTime() - baseTime.getTime();


        ItemStatus existingStatus = itemInfo.getStatus();
        ItemStatus new_status = (existingStatus != null ? existingStatus : ItemStatus.OPEN);

        if (remaining <= 0) {
            new_status = ItemStatus.CLOSED;
        } else if (remaining < AuctionMarkConstants.ITEM_ENDING_SOON) {
            new_status = ItemStatus.ENDING_SOON;
        } else if (itemInfo.getNumBids() > 0 && existingStatus != ItemStatus.CLOSED) {
            new_status = ItemStatus.WAITING_FOR_PURCHASE;
        }


        if (!new_status.equals(existingStatus)) {
            switch (new_status) {
                case OPEN:
                    this.addItem(this.items_available, itemInfo);
                    break;
                case ENDING_SOON:
                    this.items_available.remove(itemInfo);
                    this.addItem(this.items_endingSoon, itemInfo);
                    break;
                case WAITING_FOR_PURCHASE:
                    (existingStatus == ItemStatus.OPEN ? this.items_available : this.items_endingSoon).remove(itemInfo);
                    this.addItem(this.items_waitingForPurchase, itemInfo);
                    break;
                case CLOSED:
                    if (existingStatus == ItemStatus.OPEN) {
                        this.items_available.remove(itemInfo);
                    } else if (existingStatus == ItemStatus.ENDING_SOON) {
                        this.items_endingSoon.remove(itemInfo);
                    } else {
                        this.items_waitingForPurchase.remove(itemInfo);
                    }
                    this.addItem(this.items_completed, itemInfo);
                    break;
                default:

            }
            itemInfo.setStatus(new_status);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("%s - #%s [%s]", new_status, itemInfo.getItemId().encode(), itemInfo.getEndDate()));
        }

        return (new_status);
    }

    /**
     * @param itemSet
     * @param needCurrentPrice
     * @param needFutureEndDate TODO
     * @return
     */
    private ItemInfo getRandomItem(LinkedList<ItemInfo> itemSet, boolean needCurrentPrice, boolean needFutureEndDate) {
        Timestamp currentTime = this.updateAndGetCurrentTime();
        int num_items = itemSet.size();
        int idx = -1;
        ItemInfo itemInfo = null;

        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Getting random ItemInfo [numItems=%d, currentTime=%s, needCurrentPrice=%s]",
                    num_items, currentTime, needCurrentPrice));
        }
        long tries = 1000;
        tmp_seenItems.clear();
        while (num_items > 0 && tries-- > 0 && tmp_seenItems.size() < num_items) {
            idx = this.rng.nextInt(num_items);
            ItemInfo temp = itemSet.get(idx);

            if (tmp_seenItems.contains(temp)) {
                continue;
            }
            tmp_seenItems.add(temp);

            // Needs to have an embedded currentPrice
            if (needCurrentPrice && !temp.hasCurrentPrice()) {
                continue;
            }

            // If they want an item that is ending in the future, then we compare it with 
            // the current timestamp
            if (needFutureEndDate) {
                boolean compareTo = (temp.getEndDate().compareTo(currentTime) < 0);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("CurrentTime:{} / EndTime:{} [compareTo={}]", currentTime, temp.getEndDate(), compareTo);
                }
                if (!temp.hasEndDate() || compareTo) {
                    continue;
                }
            }

            // Uniform
            itemInfo = temp;
            break;
        }
        if (itemInfo == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to find ItemInfo [hasCurrentPrice={}, needFutureEndDate={}]", needCurrentPrice, needFutureEndDate);
            }
            return (null);
        }


        // Take the item out of the set and insert back to the front
        // This is so that we can maintain MRU->LRU ordering
        itemSet.remove(idx);
        itemSet.addFirst(itemInfo);

        return itemInfo;
    }

    /**********************************************************************************************
     * AVAILABLE ITEMS
     **********************************************************************************************/
    public ItemInfo getRandomAvailableItemId() {
        return this.getRandomItem(this.items_available, false, false);
    }

    public ItemInfo getRandomAvailableItem(boolean hasCurrentPrice) {
        return this.getRandomItem(this.items_available, hasCurrentPrice, false);
    }

    public int getAvailableItemsCount() {
        return this.items_available.size();
    }

    /**********************************************************************************************
     * ENDING SOON ITEMS
     **********************************************************************************************/
    public ItemInfo getRandomEndingSoonItem() {
        return this.getRandomItem(this.items_endingSoon, false, true);
    }

    public ItemInfo getRandomEndingSoonItem(boolean hasCurrentPrice) {
        return this.getRandomItem(this.items_endingSoon, hasCurrentPrice, true);
    }

    public int getEndingSoonItemsCount() {
        return this.items_endingSoon.size();
    }

    /**********************************************************************************************
     * WAITING FOR PURCHASE ITEMS
     **********************************************************************************************/
    public ItemInfo getRandomWaitForPurchaseItem() {
        return this.getRandomItem(this.items_waitingForPurchase, false, false);
    }

    public int getWaitForPurchaseItemsCount() {
        return this.items_waitingForPurchase.size();
    }

    /**********************************************************************************************
     * COMPLETED ITEMS
     **********************************************************************************************/
    public ItemInfo getRandomCompleteItem() {
        return this.getRandomItem(this.items_completed, false, false);
    }

    public int getCompleteItemsCount() {
        return this.items_completed.size();
    }

    /**********************************************************************************************
     * ALL ITEMS
     **********************************************************************************************/
    public int getAllItemsCount() {
        return (this.getAvailableItemsCount() +
                this.getEndingSoonItemsCount() +
                this.getWaitForPurchaseItemsCount() +
                this.getCompleteItemsCount());
    }

    public ItemInfo getRandomItem() {

        int idx = -1;
        while (idx == -1 || allItemSets[idx].isEmpty()) {
            idx = rng.nextInt(allItemSets.length);
        }
        return (this.getRandomItem(allItemSets[idx], false, false));
    }

    // ----------------------------------------------------------------
    // GLOBAL ATTRIBUTE METHODS
    // ----------------------------------------------------------------

    /**
     * Return a random GlobalAttributeValueId
     *
     * @return
     */
    public GlobalAttributeValueId getRandomGlobalAttributeValue() {
        int offset = rng.nextInt(this.gag_ids.size());
        GlobalAttributeGroupId gag_id = this.gag_ids.get(offset);

        int count = rng.nextInt(gag_id.getCount());
        return new GlobalAttributeValueId(gag_id, count);
    }

    public int getRandomCategoryId() {
        if (this.randomCategory == null) {
            this.randomCategory = new FlatHistogram<>(this.rng, this.items_per_category);
        }
        return randomCategory.nextInt();
    }

    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<>();
        m.put("Scale Factor", this.scale_factor);
        m.put("Loader Start", this.loaderStartTime);
        m.put("Loader Stop", this.loaderStopTime);
        m.put("Last CloseAuctions", (this.lastCloseAuctionsTime.getTime() > 0 ? this.lastCloseAuctionsTime : null));
        m.put("Client Start", this.clientStartTime);
        m.put("Current Virtual Time", this.currentTime);
        m.put("Pending ItemCommentResponses", this.pending_commentResponses.size());

        // Item Queues
        Histogram<ItemStatus> itemCounts = new Histogram<>(true);
        for (ItemStatus status : ItemStatus.values()) {
            int cnt = 0;
            switch (status) {
                case OPEN:
                    cnt = this.items_available.size();
                    break;
                case ENDING_SOON:
                    cnt = this.items_endingSoon.size();
                    break;
                case WAITING_FOR_PURCHASE:
                    cnt = this.items_waitingForPurchase.size();
                    break;
                case CLOSED:
                    cnt = this.items_completed.size();
                    break;
                default:

            }
            itemCounts.put(status, cnt);
        }
        m.put("Item Queues", itemCounts);

        return (StringUtil.formatMaps(m));
    }

}
