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

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.auctionmark.util.*;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.*;
import com.oltpbenchmark.util.RandomDistribution.Flat;
import com.oltpbenchmark.util.RandomDistribution.Zipf;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @author pavlo
 * @author visawee
 */
public class AuctionMarkLoader extends Loader<AuctionMarkBenchmark> {

    // -----------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // -----------------------------------------------------------------

    protected final AuctionMarkProfile profile;

    /**
     * Data Generator Classes TableName -> AbstactTableGenerator
     */
    private final Map<String, AbstractTableGenerator> generators = Collections.synchronizedMap(new ListOrderedMap<>());


    /**
     * The set of tables that we have finished loading
     **/
    private final transient Collection<String> finished = Collections.synchronizedCollection(new HashSet<>());

    private final Histogram<String> tableSizes = new Histogram<>();


    // -----------------------------------------------------------------
    // INITIALIZATION
    // -----------------------------------------------------------------

    public AuctionMarkLoader(AuctionMarkBenchmark benchmark) {
        super(benchmark);

        // BenchmarkProfile
        this.profile = new AuctionMarkProfile(benchmark, benchmark.getRandomGenerator());


        try {

            // ---------------------------
            // Fixed-Size Table Generators
            // ---------------------------

            this.registerGenerator(new RegionGenerator());
            this.registerGenerator(new CategoryGenerator());
            this.registerGenerator(new GlobalAttributeGroupGenerator());
            this.registerGenerator(new GlobalAttributeValueGenerator());

            // ---------------------------
            // Scaling-Size Table Generators
            // ---------------------------

            // depends on REGION
            this.registerGenerator(new UserGenerator());
            // depends on USERACCT
            this.registerGenerator(new UserAttributesGenerator());

            // depends on USERACCT, CATEGORY
            this.registerGenerator(new ItemGenerator());
            // depends on ITEM
            this.registerGenerator(new ItemCommentGenerator());
            // depends on ITEM
            this.registerGenerator(new ItemImageGenerator());
            // depends on ITEM
            this.registerGenerator(new ItemBidGenerator());

            // depends on ITEM, GLOBAL_ATTRIBUTE_GROUP, GLOBAL_ATTRIBUTE_VALUE
            this.registerGenerator(new ItemAttributeGenerator());

            // depends on ITEM_BID
            this.registerGenerator(new ItemMaxBidGenerator());
            // depends on ITEM_BID
            this.registerGenerator(new ItemPurchaseGenerator());
            // depends on ITEM_BID
            this.registerGenerator(new UserItemGenerator());
            // depends on ITEM_BID
            this.registerGenerator(new UserWatchGenerator());

            // depends on ITEM_PURCHASE
            this.registerGenerator(new UserFeedbackGenerator());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // -----------------------------------------------------------------
    // LOADING METHODS
    // -----------------------------------------------------------------

    private class CountdownLoaderThread extends LoaderThread {
        private final AbstractTableGenerator generator;
        private final CountDownLatch latch;

        public CountdownLoaderThread(BenchmarkModule benchmarkModule, AbstractTableGenerator generator, CountDownLatch latch) {
            super(benchmarkModule);
            this.generator = generator;
            this.latch = latch;
        }

        @Override
        public void load(Connection conn) throws SQLException {
            LOG.debug(String.format("Started loading %s which depends on %s", this.generator.getTableName(), this.generator.getDependencies()));
            this.generator.load(conn);
            LOG.debug(String.format("Finished loading %s", this.generator.getTableName()));
        }

        @Override
        public void beforeLoad() {
            this.generator.beforeLoad();
        }

        @Override
        public void afterLoad() {
            this.generator.afterLoad();
            this.latch.countDown();
        }
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();

        final CountDownLatch loadLatch = new CountDownLatch(this.generators.size());

        for (AbstractTableGenerator generator : this.generators.values()) {
            generator.init();
            threads.add(new CountdownLoaderThread(this.benchmark, generator, loadLatch));
        }

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

    private void registerGenerator(AbstractTableGenerator generator) {
        // Register this one as well as any sub-generators
        this.generators.put(generator.getTableName(), generator);
        for (AbstractTableGenerator sub_generator : generator.getSubTableGenerators()) {
            this.registerGenerator(sub_generator);
        }
    }

    protected AbstractTableGenerator getGenerator(String table_name) {
        return (this.generators.get(table_name));
    }

    protected void generateTableData(Connection conn, String tableName) throws SQLException {
        LOG.debug("*** START {}", tableName);
        final AbstractTableGenerator generator = this.generators.get(tableName);


        // Generate Data
        final Table catalog_tbl = benchmark.getCatalog().getTable(tableName);

        final List<Object[]> volt_table = generator.getVoltTable();
        final String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        boolean shouldExecuteBatch = false;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            final int[] types = catalog_tbl.getColumnTypes();

            while (generator.hasMore()) {
                generator.generateBatch();

                for (Object[] row : volt_table) {
                    for (int i = 0; i < row.length; i++) {
                        if (row[i] != null) {
                            stmt.setObject(i + 1, row[i]);
                        } else {
                            stmt.setNull(i + 1, types[i]);
                        }
                    }
                    stmt.addBatch();
                    shouldExecuteBatch = true;
                }

                if (shouldExecuteBatch) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    shouldExecuteBatch = false;
                }


                this.tableSizes.put(tableName, volt_table.size());

                // Release anything to the sub-generators if we have it
                // We have to do this to ensure that all of the parent tuples get
                // insert first for foreign-key relationships
                generator.releaseHoldsToSubTableGenerators();
            }
        }


        generator.markAsFinished();

        synchronized (this) {
            this.finished.add(tableName);
            LOG.debug(String.format("*** FINISH %s - %d tuples - [%d / %d]", tableName, this.tableSizes.get(tableName), this.finished.size(), this.generators.size()));
            if (LOG.isDebugEnabled()) {
                LOG.debug("Remaining Tables: {}", CollectionUtils.subtract(this.generators.keySet(), this.finished));
            }
        }

    }

    /**********************************************************************************************
     * AbstractTableGenerator
     **********************************************************************************************/
    protected abstract class AbstractTableGenerator extends LoaderThread {
        private final String tableName;
        private final Table catalog_tbl;
        protected final List<Object[]> table = new ArrayList<>();
        protected Long tableSize;
        protected int batchSize;
        protected final CountDownLatch latch = new CountDownLatch(1);
        protected final List<String> dependencyTables = new ArrayList<>();

        /**
         * Some generators have children tables that we want to load tuples for each batch of this generator.
         * The queues we need to update every time we generate a new LoaderItemInfo
         */
        protected final Set<SubTableGenerator<?>> sub_generators = new HashSet<>();

        protected final List<Object> subGenerator_hold = new ArrayList<>();

        protected long count = 0;

        /**
         * Any column with the name XX_SATTR## will automatically be filled with a random string
         */
        protected final List<Column> random_str_cols = new ArrayList<>();
        protected final Pattern random_str_regex = Pattern.compile("[\\w]+\\_SATTR[\\d]+", Pattern.CASE_INSENSITIVE);

        /**
         * Any column with the name XX_IATTR## will automatically be filled with a random integer
         */
        protected List<Column> random_int_cols = new ArrayList<>();
        protected final Pattern random_int_regex = Pattern.compile("[\\w]+\\_IATTR[\\d]+", Pattern.CASE_INSENSITIVE);

        public AbstractTableGenerator(String tableName, String... dependencies) {
            super(benchmark);
            this.tableName = tableName;
            this.catalog_tbl = benchmark.getCatalog().getTable(tableName);
            this.batchSize = workConf.getBatchSize();


            boolean fixed_size = AuctionMarkConstants.FIXED_TABLES.contains(catalog_tbl.getName().toLowerCase());
            boolean dynamic_size = AuctionMarkConstants.DYNAMIC_TABLES.contains(catalog_tbl.getName().toLowerCase());
            boolean data_file = AuctionMarkConstants.DATAFILE_TABLES.contains(catalog_tbl.getName().toLowerCase());

            // Add the dependencies so that we know what we need to block on
            CollectionUtil.addAll(this.dependencyTables, dependencies);

            // Initialize dynamic parameters for tables that are not loaded from data files
            if (!data_file && !dynamic_size && !tableName.equalsIgnoreCase(AuctionMarkConstants.TABLENAME_ITEM)) {
                String field_name = "TABLESIZE_" + catalog_tbl.getName().toUpperCase();
                try {

                    Field field_handle = AuctionMarkConstants.class.getField(field_name);

                    this.tableSize = (Long) field_handle.get(null);
                    if (!fixed_size) {
                        this.tableSize = (long) Math.max(1, (int) Math.round(this.tableSize * profile.getScaleFactor()));
                    }
                } catch (NoSuchFieldException ex) {
                    LOG.warn("No table size constant in AuctionMarkConstants for [{}]", field_name);
                } catch (Exception ex) {
                    throw new RuntimeException("Missing field '" + field_name + "' needed for '" + tableName + "'", ex);
                }
            }

            for (Column catalog_col : this.catalog_tbl.getColumns()) {
                if (random_str_regex.matcher(catalog_col.getName().toUpperCase()).matches()) {

                    this.random_str_cols.add(catalog_col);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Random String Column: {}", catalog_col.getName().toLowerCase());
                    }
                } else if (random_int_regex.matcher(catalog_col.getName().toUpperCase()).matches()) {

                    this.random_int_cols.add(catalog_col);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Random Integer Column: {}", catalog_col.getName().toLowerCase());
                    }
                }
            }
            if (LOG.isDebugEnabled()) {
                if (this.random_str_cols.size() > 0) {
                    LOG.debug(String.format("%s Random String Columns: %s", tableName, this.random_str_cols));
                }
                if (this.random_int_cols.size() > 0) {
                    LOG.debug(String.format("%s Random Integer Columns: %s", tableName, this.random_int_cols));
                }
            }
        }

        /**
         * Initiate data that need dependencies
         */
        public abstract void init();

        /**
         * Prepare to generate tuples
         */
        public abstract void prepare();

        /**
         * All sub-classes must implement this. This will enter new tuple data into the row
         *
         * @param row TODO
         */
        protected abstract int populateRow(Object[] row);

        @Override
        public void load(Connection conn) {
            // Then invoke the loader generation method
            try {
                AuctionMarkLoader.this.generateTableData(conn, this.tableName);
            } catch (Throwable ex) {
                throw new RuntimeException("Unexpected error while generating table data for '" + this.tableName + "'", ex);
            }
        }

        @Override
        public void beforeLoad() {
            // First block on the CountDownLatches of all the tables that we depend on
            if (this.dependencyTables.size() > 0 && LOG.isDebugEnabled()) {
                LOG.debug(String.format("%s: Table generator is blocked waiting for %d other tables: %s", this.tableName, this.dependencyTables.size(), this.dependencyTables));
            }
            for (String dependency : this.dependencyTables) {
                AbstractTableGenerator gen = AuctionMarkLoader.this.generators.get(dependency);

                try {
                    gen.latch.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException("Unexpected interruption for '" + this.tableName + "' waiting for '" + dependency + "'", ex);
                }
            }

            // Make sure we call prepare before we start generating table data
            this.prepare();
        }

        @SuppressWarnings("unchecked")
        public <T extends AbstractTableGenerator> T addSubTableGenerator(SubTableGenerator<?> sub_item) {
            this.sub_generators.add(sub_item);
            return ((T) this);
        }

        @SuppressWarnings("unchecked")
        public void releaseHoldsToSubTableGenerators() {
            if (!this.subGenerator_hold.isEmpty()) {
                LOG.trace(String.format("%s: Releasing %d held objects to %d sub-generators", this.tableName, this.subGenerator_hold.size(), this.sub_generators.size()));
                for (@SuppressWarnings("rawtypes") SubTableGenerator sub_generator : this.sub_generators) {
                    sub_generator.queue.addAll(this.subGenerator_hold);
                }
                this.subGenerator_hold.clear();
            }
        }

        public void updateSubTableGenerators(Object obj) {
            // Queue up this item for our multi-threaded sub-generators
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("%s: Updating %d sub-generators with %s: %s", this.tableName, this.sub_generators.size(), obj, this.sub_generators));
            }
            this.subGenerator_hold.add(obj);
        }

        public Collection<SubTableGenerator<?>> getSubTableGenerators() {
            return (this.sub_generators);
        }

        public Collection<String> getSubGeneratorTableNames() {
            List<String> names = new ArrayList<>();
            for (AbstractTableGenerator gen : this.sub_generators) {
                names.add(gen.catalog_tbl.getName().toLowerCase());
            }
            return (names);
        }

        protected int populateRandomColumns(Object[] row) {
            int cols = 0;

            // STRINGS
            for (Column catalog_col : this.random_str_cols) {
                int size = catalog_col.getSize();
                row[catalog_col.getIndex()] = profile.rng.astring(profile.rng.nextInt(size - 1), size);
                cols++;
            }

            // INTEGER
            for (Column catalog_col : this.random_int_cols) {
                row[catalog_col.getIndex()] = profile.rng.number(0, 1 << 30);
                cols++;
            }

            return (cols);
        }

        public synchronized boolean hasMore() {
            return (this.count < this.tableSize);
        }

        public Table getTableCatalog() {
            return (this.catalog_tbl);
        }

        public List<Object[]> getVoltTable() {
            return this.table;
        }

        public Long getTableSize() {
            return this.tableSize;
        }

        public int getBatchSize() {
            return this.batchSize;
        }

        public String getTableName() {
            return this.tableName;
        }

        public synchronized long getCount() {
            return this.count;
        }

        /**
         * When called, the generator will populate a new row record and append it to the underlying VoltTable
         */
        public synchronized void addRow() {
            Object[] row = new Object[this.catalog_tbl.getColumnCount()];

            // Main Columns
            int cols = this.populateRow(row);

            // RANDOM COLS
            cols += this.populateRandomColumns(row);

            // Convert all CompositeIds into their long encodings
            for (int i = 0; i < cols; i++) {
                if (row[i] != null && row[i] instanceof CompositeId) {
                    row[i] = ((CompositeId) row[i]).encode();
                }
            }

            this.count++;
            this.table.add(row);
        }

        /**
         *
         */
        public void generateBatch() {
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("%s: Generating new batch", this.getTableName()));
            }
            long batch_count = 0;
            this.table.clear();
            while (this.hasMore() && this.table.size() < this.batchSize) {
                this.addRow();
                batch_count++;
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("%s: Finished generating new batch of %d tuples", this.getTableName(), batch_count));
            }
        }

        public void markAsFinished() {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("%s: Marking as finished", this.tableName));
            }
            this.latch.countDown();
            for (SubTableGenerator<?> sub_generator : this.sub_generators) {
                sub_generator.stopWhenEmpty();
            }
        }

        public boolean isFinish() {
            return (this.latch.getCount() == 0);
        }

        public List<String> getDependencies() {
            return this.dependencyTables;
        }

        @Override
        public String toString() {
            return String.format("Generator[%s]", this.tableName);
        }
    }

    /**********************************************************************************************
     * SubUserTableGenerator
     * This is for tables that are based off of the USER table
     **********************************************************************************************/
    protected abstract class SubTableGenerator<T> extends AbstractTableGenerator {

        private final LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<>();
        private T current;
        private int currentCounter;
        private boolean stop = false;
        private final String sourceTableName;

        public SubTableGenerator(String tableName, String sourceTableName, String... dependencies) throws SQLException {
            super(tableName, dependencies);
            this.sourceTableName = sourceTableName;
        }

        protected abstract int getElementCounter(T t);

        protected abstract int populateRow(T t, Object[] row, int remaining);

        public void stopWhenEmpty() {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("%s: Will stop when queue is empty", this.getTableName()));
            }
            this.stop = true;
        }

        @Override
        public void init() {
            // Get the AbstractTableGenerator that will feed into this generator
            AbstractTableGenerator parent_gen = AuctionMarkLoader.this.generators.get(this.sourceTableName);

            parent_gen.addSubTableGenerator(this);

            this.current = null;
            this.currentCounter = 0;
        }

        @Override
        public void prepare() {
            // Nothing to do...
        }

        @Override
        public final boolean hasMore() {
            return (this.getNext() != null);
        }

        @Override
        protected final int populateRow(Object[] row) {
            T t = this.getNext();

            this.currentCounter--;
            return (this.populateRow(t, row, this.currentCounter));
        }

        private T getNext() {
            T last = this.current;
            if (this.current == null || this.currentCounter == 0) {
                while (this.currentCounter == 0) {
                    try {
                        this.current = this.queue.poll(1000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ex) {
                        return (null);
                    }
                    // Check whether we should stop
                    if (this.current == null) {
                        if (this.stop) {
                            break;
                        }
                        continue;
                    }
                    this.currentCounter = this.getElementCounter(this.current);
                }
            }
            if (last != this.current) {
                if (last != null) {
                    this.finishElementCallback(last);
                }
                if (this.current != null) {
                    this.newElementCallback(this.current);
                }
            }
            return this.current;
        }

        protected void finishElementCallback(T t) {
            // Nothing...
        }

        protected void newElementCallback(T t) {
            // Nothing...
        }
    }

    /**********************************************************************************************
     * REGION Generator
     **********************************************************************************************/
    protected class RegionGenerator extends AbstractTableGenerator {

        public RegionGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_REGION);
        }

        @Override
        public void init() {
            // Nothing to do
        }

        @Override
        public void prepare() {
            // Nothing to do
        }

        @Override
        protected int populateRow(Object[] row) {
            int col = 0;

            // R_ID
            row[col++] = (int) this.count;
            // R_NAME
            row[col++] = profile.rng.astring(6, 32);

            return (col);
        }
    }

    /**********************************************************************************************
     * CATEGORY Generator
     **********************************************************************************************/
    protected class CategoryGenerator extends AbstractTableGenerator {
        private final Map<String, Category> categoryMap;
        private final LinkedList<Category> categories = new LinkedList<>();

        public CategoryGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_CATEGORY);

            this.categoryMap = (new CategoryParser()).getCategoryMap();
            this.tableSize = (long) this.categoryMap.size();
        }

        @Override
        public void init() {
            for (Category category : this.categoryMap.values()) {
                if (category.isLeaf()) {
                    profile.items_per_category.put(category.getCategoryID(), category.getItemCount());
                }
                this.categories.add(category);
            }
        }

        @Override
        public void prepare() {
            // Nothing to do
        }

        @Override
        protected int populateRow(Object[] row) {
            int col = 0;

            Category category = this.categories.poll();


            // C_ID
            row[col++] = category.getCategoryID();
            // C_NAME
            row[col++] = category.getName().toLowerCase();
            // C_PARENT_ID
            row[col++] = category.getParentCategoryID();

            return (col);
        }
    }

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_GROUP Generator
     **********************************************************************************************/
    protected class GlobalAttributeGroupGenerator extends AbstractTableGenerator {
        private final Histogram<Integer> category_groups = new Histogram<>();
        private final LinkedList<GlobalAttributeGroupId> group_ids = new LinkedList<>();

        public GlobalAttributeGroupGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP, AuctionMarkConstants.TABLENAME_CATEGORY);
        }

        @Override
        public void init() {
            // Nothing to do
        }

        @Override
        public void prepare() {
            // Grab the number of CATEGORY items that we have inserted
            long num_categories = getGenerator(AuctionMarkConstants.TABLENAME_CATEGORY).tableSize;

            for (int i = 0; i < this.tableSize; i++) {
                int category_id = profile.rng.number(0, ((int) num_categories - 1));
                this.category_groups.put(category_id);
                int id = this.category_groups.get(category_id);
                int count = (int) profile.rng.number(1, AuctionMarkConstants.TABLESIZE_GLOBAL_ATTRIBUTE_VALUE_PER_GROUP);
                GlobalAttributeGroupId gag_id = new GlobalAttributeGroupId(category_id, id, count);

                profile.gag_ids.add(gag_id);
                this.group_ids.add(gag_id);
            }
        }

        @Override
        protected int populateRow(Object[] row) {
            int col = 0;

            GlobalAttributeGroupId gag_id = this.group_ids.poll();

            // GAG_ID
            row[col++] = gag_id.encode();
            // GAG_C_ID
            row[col++] = gag_id.getCategoryId();
            // GAG_NAME
            row[col++] = profile.rng.astring(6, 32);

            return (col);
        }
    }

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_VALUE Generator
     **********************************************************************************************/
    protected class GlobalAttributeValueGenerator extends AbstractTableGenerator {

        private final Histogram<GlobalAttributeGroupId> gag_counters = new Histogram<>(true);
        private Iterator<GlobalAttributeGroupId> gag_iterator;
        private GlobalAttributeGroupId gag_current;
        private int gav_counter = -1;

        public GlobalAttributeValueGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE, AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
        }

        @Override
        public void init() {
            // Nothing to do
        }

        @Override
        public void prepare() {
            this.tableSize = 0L;
            for (GlobalAttributeGroupId gag_id : profile.gag_ids) {
                this.gag_counters.set(gag_id, 0);
                this.tableSize += gag_id.getCount();
            }
            this.gag_iterator = profile.gag_ids.iterator();
        }

        @Override
        protected int populateRow(Object[] row) {
            int col = 0;

            if (this.gav_counter == -1 || ++this.gav_counter == this.gag_current.getCount()) {
                this.gag_current = this.gag_iterator.next();

                this.gav_counter = 0;
            }

            GlobalAttributeValueId gav_id = new GlobalAttributeValueId(this.gag_current.encode(), this.gav_counter);

            // GAV_ID
            row[col++] = gav_id.encode();
            // GAV_GAG_ID
            row[col++] = this.gag_current.encode();
            // GAV_NAME
            row[col++] = profile.rng.astring(6, 32);

            return (col);
        }
    }

    /**********************************************************************************************
     * USER Generator
     **********************************************************************************************/
    protected class UserGenerator extends AbstractTableGenerator {
        private final Zipf randomBalance;
        private final Flat randomRegion;
        private final Zipf randomRating;
        private UserIdGenerator idGenerator;

        public UserGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_USERACCT, AuctionMarkConstants.TABLENAME_REGION);
            this.randomRegion = new Flat(profile.rng, 0, (int) AuctionMarkConstants.TABLESIZE_REGION);
            this.randomRating = new Zipf(profile.rng, AuctionMarkConstants.USER_MIN_RATING, AuctionMarkConstants.USER_MAX_RATING, 1.0001);
            this.randomBalance = new Zipf(profile.rng, AuctionMarkConstants.USER_MIN_BALANCE, AuctionMarkConstants.USER_MAX_BALANCE, 1.001);
        }

        @Override
        public void init() {
            // Populate the profile's users per item count histogram so that we know how many
            // items that each user should have. This will then be used to calculate the
            // the user ids by placing them into numeric ranges
            int max_items = Math.max(1, (int) Math.ceil(AuctionMarkConstants.ITEM_ITEMS_PER_SELLER_MAX * profile.getScaleFactor()));

            LOG.debug("Max Items Per Seller: {}", max_items);
            Zipf randomNumItems = new Zipf(profile.rng, AuctionMarkConstants.ITEM_ITEMS_PER_SELLER_MIN, max_items, AuctionMarkConstants.ITEM_ITEMS_PER_SELLER_SIGMA);
            for (long i = 0; i < this.tableSize; i++) {
                long num_items = randomNumItems.nextInt();
                profile.users_per_itemCount.put(num_items);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Users Per Item Count:\n{}", profile.users_per_itemCount);
            }
            this.idGenerator = new UserIdGenerator(profile.users_per_itemCount, benchmark.getWorkloadConfiguration().getTerminals());

        }

        @Override
        public void prepare() {
            // Nothing to do
        }

        @Override
        public synchronized boolean hasMore() {
            return this.idGenerator.hasNext();
        }

        @Override
        protected int populateRow(Object[] row) {
            int col = 0;

            UserId u_id = this.idGenerator.next();

            // U_ID
            row[col++] = u_id;
            // U_RATING
            row[col++] = this.randomRating.nextInt();
            // U_BALANCE
            row[col++] = (this.randomBalance.nextInt()) / 10.0;
            // U_COMMENTS
            row[col++] = 0;
            // U_R_ID
            row[col++] = this.randomRegion.nextInt();
            // U_CREATED
            row[col++] = new Timestamp(System.currentTimeMillis());
            // U_UPDATED
            row[col++] = new Timestamp(System.currentTimeMillis());

            this.updateSubTableGenerators(u_id);
            return (col);
        }
    }

    /**********************************************************************************************
     * USER_ATTRIBUTES Generator
     **********************************************************************************************/
    protected class UserAttributesGenerator extends SubTableGenerator<UserId> {
        private final Zipf randomNumUserAttributes;

        public UserAttributesGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_USERACCT_ATTRIBUTES, AuctionMarkConstants.TABLENAME_USERACCT);

            this.randomNumUserAttributes = new Zipf(profile.rng, AuctionMarkConstants.USER_MIN_ATTRIBUTES, AuctionMarkConstants.USER_MAX_ATTRIBUTES, 1.001);
        }

        @Override
        protected int getElementCounter(UserId user_id) {
            return randomNumUserAttributes.nextInt();
        }

        @Override
        protected int populateRow(UserId user_id, Object[] row, int remaining) {
            int col = 0;

            // UA_ID
            row[col++] = this.count;
            // UA_U_ID
            row[col++] = user_id;
            // UA_NAME
            row[col++] = profile.rng.astring(AuctionMarkConstants.USER_ATTRIBUTE_NAME_LENGTH_MIN, AuctionMarkConstants.USER_ATTRIBUTE_NAME_LENGTH_MAX);
            // UA_VALUE
            row[col++] = profile.rng.astring(AuctionMarkConstants.USER_ATTRIBUTE_VALUE_LENGTH_MIN, AuctionMarkConstants.USER_ATTRIBUTE_VALUE_LENGTH_MAX);
            // U_CREATED
            row[col++] = new Timestamp(System.currentTimeMillis());

            return (col);
        }
    }

    /**********************************************************************************************
     * ITEM Generator
     **********************************************************************************************/
    protected class ItemGenerator extends SubTableGenerator<UserId> {

        /**
         * BidDurationDay -> Pair<NumberOfBids, NumberOfWatches>
         */
        private final Map<Long, Pair<Zipf, Zipf>> item_bid_watch_zipfs = new HashMap<>();

        public ItemGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_ITEM, AuctionMarkConstants.TABLENAME_USERACCT, AuctionMarkConstants.TABLENAME_USERACCT, AuctionMarkConstants.TABLENAME_CATEGORY);
        }

        @Override
        protected int getElementCounter(UserId user_id) {
            return user_id.getItemCount();
        }

        @Override
        public void init() {
            super.init();
            this.tableSize = 0L;
            for (Long size : profile.users_per_itemCount.values()) {
                this.tableSize += size * profile.users_per_itemCount.get(size);
            }
        }

        @Override
        protected int populateRow(UserId seller_id, Object[] row, int remaining) {
            int col = 0;

            ItemId itemId = new ItemId(seller_id, remaining);
            Timestamp endDate = this.getRandomEndTimestamp();
            Timestamp startDate = this.getRandomStartTimestamp(endDate);
            if (LOG.isTraceEnabled()) {
                LOG.trace("endDate = {} : startDate = {}", endDate, startDate);
            }

            long bidDurationDay = ((endDate.getTime() - startDate.getTime()) / AuctionMarkConstants.MILLISECONDS_IN_A_DAY);
            Pair<Zipf, Zipf> p = this.item_bid_watch_zipfs.get(bidDurationDay);
            if (p == null) {
                Zipf randomNumBids = new Zipf(profile.rng, AuctionMarkConstants.ITEM_BIDS_PER_DAY_MIN * bidDurationDay, AuctionMarkConstants.ITEM_BIDS_PER_DAY_MAX * bidDurationDay, AuctionMarkConstants.ITEM_BIDS_PER_DAY_SIGMA);
                Zipf randomNumWatches = new Zipf(profile.rng, AuctionMarkConstants.ITEM_WATCHES_PER_DAY_MIN * bidDurationDay, AuctionMarkConstants.ITEM_WATCHES_PER_DAY_MAX * bidDurationDay, AuctionMarkConstants.ITEM_WATCHES_PER_DAY_SIGMA);
                p = Pair.of(randomNumBids, randomNumWatches);
                this.item_bid_watch_zipfs.put(bidDurationDay, p);
            }


            // Calculate the number of bids and watches for this item
            int numBids = p.first.nextInt();
            int numWatches = p.second.nextInt();

            // Create the ItemInfo object that we will use to cache the local data

            // tables are done with it.
            LoaderItemInfo itemInfo = new LoaderItemInfo(itemId, endDate, numBids);
            itemInfo.setStartDate(startDate);
            itemInfo.setInitialPrice(profile.randomInitialPrice.nextInt());

            itemInfo.setNumImages((short) profile.randomNumImages.nextInt());
            itemInfo.setNumAttributes((short) profile.randomNumAttributes.nextInt());
            itemInfo.setNumBids(numBids);
            itemInfo.setNumWatches(numWatches);

            // The auction for this item has already closed
            if (itemInfo.getEndDate().getTime() <= profile.getLoaderStartTime().getTime()) {
                // Somebody won a bid and bought the item
                if (itemInfo.getNumBids() > 0) {
                    itemInfo.setLastBidderId(profile.getRandomBuyerId(itemInfo.getSellerId()));
                    itemInfo.setPurchaseDate(this.getRandomPurchaseTimestamp(itemInfo.getEndDate()));
                    itemInfo.setNumComments((short) profile.randomNumComments.nextInt());
                }
                itemInfo.setStatus(ItemStatus.CLOSED);
            }
            // Item is still available
            else if (itemInfo.getNumBids() > 0) {
                itemInfo.setLastBidderId(profile.getRandomBuyerId(itemInfo.getSellerId()));
            }
            profile.addItemToProperQueue(itemInfo, true);

            // I_ID
            row[col++] = itemInfo.getItemId();
            // I_U_ID
            row[col++] = itemInfo.getSellerId();
            // I_C_ID
            row[col++] = profile.getRandomCategoryId();
            // I_NAME
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_NAME_LENGTH_MIN, AuctionMarkConstants.ITEM_NAME_LENGTH_MAX);
            // I_DESCRIPTION
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_DESCRIPTION_LENGTH_MIN, AuctionMarkConstants.ITEM_DESCRIPTION_LENGTH_MAX);
            // I_USER_ATTRIBUTES
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_USER_ATTRIBUTES_LENGTH_MIN, AuctionMarkConstants.ITEM_USER_ATTRIBUTES_LENGTH_MAX);
            // I_INITIAL_PRICE
            row[col++] = itemInfo.getInitialPrice();

            // I_CURRENT_PRICE
            if (itemInfo.getNumBids() > 0) {
                itemInfo.setCurrentPrice(itemInfo.getInitialPrice() + (itemInfo.getNumBids() * itemInfo.getInitialPrice() * AuctionMarkConstants.ITEM_BID_PERCENT_STEP));
                row[col++] = itemInfo.getCurrentPrice();
            } else {
                row[col++] = itemInfo.getInitialPrice();
            }

            // I_NUM_BIDS
            row[col++] = itemInfo.getNumBids();
            // I_NUM_IMAGES
            row[col++] = itemInfo.getNumImages();
            // I_NUM_GLOBAL_ATTRS
            row[col++] = itemInfo.getNumAttributes();
            // I_NUM_COMMENTS
            row[col++] = itemInfo.getNumComments();
            // I_START_DATE
            row[col++] = itemInfo.getStartDate();
            // I_END_DATE
            row[col++] = itemInfo.getEndDate();
            // I_STATUS
            row[col++] = itemInfo.getStatus().ordinal();
            // I_CREATED
            row[col++] = profile.getLoaderStartTime();
            // I_UPDATED
            row[col++] = itemInfo.getStartDate();

            this.updateSubTableGenerators(itemInfo);
            return (col);
        }

        private Timestamp getRandomStartTimestamp(Timestamp endDate) {
            long duration = ((long) profile.randomDuration.nextInt()) * AuctionMarkConstants.MILLISECONDS_IN_A_DAY;
            long lStartTimestamp = endDate.getTime() - duration;
            return new Timestamp(lStartTimestamp);
        }

        private Timestamp getRandomEndTimestamp() {
            int timeDiff = profile.randomTimeDiff.nextInt();
            return new Timestamp(profile.getLoaderStartTime().getTime() + (timeDiff * AuctionMarkConstants.MILLISECONDS_IN_A_SECOND));
        }

        private Timestamp getRandomPurchaseTimestamp(Timestamp endDate) {
            long duration = profile.randomPurchaseDuration.nextInt();
            return new Timestamp(endDate.getTime() + duration * AuctionMarkConstants.MILLISECONDS_IN_A_DAY);
        }
    }

    /**********************************************************************************************
     * ITEM_IMAGE Generator
     **********************************************************************************************/
    protected class ItemImageGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemImageGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_ITEM_IMAGE, AuctionMarkConstants.TABLENAME_ITEM);
        }

        @Override
        public int getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.getNumImages();
        }

        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, int remaining) {
            int col = 0;

            // II_ID
            row[col++] = this.count;
            // II_I_ID
            row[col++] = itemInfo.getItemId();
            // II_U_ID
            row[col++] = itemInfo.getSellerId();

            return (col);
        }
    }

    /**********************************************************************************************
     * ITEM_ATTRIBUTE Generator
     **********************************************************************************************/
    protected class ItemAttributeGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemAttributeGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE, AuctionMarkConstants.TABLENAME_ITEM, AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP, AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE);
        }

        @Override
        public int getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.getNumAttributes();
        }

        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, int remaining) {
            int col = 0;
            GlobalAttributeValueId gav_id = profile.getRandomGlobalAttributeValue();


            // IA_ID
            row[col++] = this.count;
            // IA_I_ID
            row[col++] = itemInfo.getItemId();
            // IA_U_ID
            row[col++] = itemInfo.getSellerId();
            // IA_GAV_ID
            row[col++] = gav_id.encode();
            // IA_GAG_ID
            row[col++] = gav_id.getGlobalAttributeGroup().encode();

            return (col);
        }
    }

    /**********************************************************************************************
     * ITEM_COMMENT Generator
     **********************************************************************************************/
    protected class ItemCommentGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemCommentGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_ITEM_COMMENT, AuctionMarkConstants.TABLENAME_ITEM);
        }

        @Override
        public int getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.getPurchaseDate() != null ? itemInfo.getNumComments() : 0;
        }

        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, int remaining) {
            int col = 0;

            // IC_ID
            row[col++] = (int) this.count;
            // IC_I_ID
            row[col++] = itemInfo.getItemId();
            // IC_U_ID
            row[col++] = itemInfo.getSellerId();
            // IC_BUYER_ID
            row[col++] = itemInfo.getLastBidderId();
            // IC_QUESTION
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_COMMENT_LENGTH_MIN, AuctionMarkConstants.ITEM_COMMENT_LENGTH_MAX);
            // IC_RESPONSE
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_COMMENT_LENGTH_MIN, AuctionMarkConstants.ITEM_COMMENT_LENGTH_MAX);
            // IC_CREATED
            row[col++] = this.getRandomCommentDate(itemInfo.getStartDate(), itemInfo.getEndDate());
            // IC_UPDATED
            row[col++] = this.getRandomCommentDate(itemInfo.getStartDate(), itemInfo.getEndDate());

            return (col);
        }

        private Timestamp getRandomCommentDate(Timestamp startDate, Timestamp endDate) {
            int start = Math.round(startDate.getTime() / AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
            int end = Math.round(endDate.getTime() / AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
            return new Timestamp((profile.rng.number(start, end)) * AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
        }
    }

    /**********************************************************************************************
     * ITEM_BID Generator
     **********************************************************************************************/
    protected class ItemBidGenerator extends SubTableGenerator<LoaderItemInfo> {

        private LoaderItemInfo.Bid bid = null;
        private float currentBidPriceAdvanceStep;
        private long currentCreateDateAdvanceStep;
        private float currentPrice;
        private boolean new_item;

        public ItemBidGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_ITEM_BID, AuctionMarkConstants.TABLENAME_ITEM);
        }

        @Override
        public int getElementCounter(LoaderItemInfo itemInfo) {
            return ((int) itemInfo.getNumBids());
        }

        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, int remaining) {
            int col = 0;


            UserId bidderId = null;

            // Figure out the UserId for the person bidding on this item now
            if (this.new_item) {
                // If this is a new item and there is more than one bid, then
                // we'll choose the bidder's UserId at random.
                // If there is only one bid, then it will have to be the last bidder
                bidderId = (itemInfo.getNumBids() == 1 ? itemInfo.getLastBidderId() : profile.getRandomBuyerId(itemInfo.getSellerId()));
                Timestamp endDate;
                if (itemInfo.getStatus().equals(ItemStatus.OPEN)) {
                    endDate = profile.getLoaderStartTime();
                } else {
                    endDate = itemInfo.getEndDate();
                }
                this.currentCreateDateAdvanceStep = (endDate.getTime() - itemInfo.getStartDate().getTime()) / (remaining + 1);
                this.currentBidPriceAdvanceStep = itemInfo.getInitialPrice() * AuctionMarkConstants.ITEM_BID_PERCENT_STEP;
                this.currentPrice = itemInfo.getInitialPrice();
            }
            // The last bid must always be the item's lastBidderId
            else if (remaining == 0) {
                bidderId = itemInfo.getLastBidderId();
                this.currentPrice = itemInfo.getCurrentPrice();
            }
            // The first bid for a two-bid item must always be different than the lastBidderId
            else if (itemInfo.getNumBids() == 2) {

                bidderId = profile.getRandomBuyerId(itemInfo.getLastBidderId(), itemInfo.getSellerId());
            }
            // Since there are multiple bids, we want randomly select one based on the previous bidders
            // We will get the histogram of bidders so that we are more likely to select
            // an existing bidder rather than a completely random one
            else {

                Histogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
                bidderId = profile.getRandomBuyerId(bidderHistogram, this.bid.getBidderId(), itemInfo.getSellerId());
                this.currentPrice += this.currentBidPriceAdvanceStep;
            }


            float last_bid = (this.new_item ? itemInfo.getInitialPrice() : this.bid.getMaxBid());
            this.bid = itemInfo.getNextBid(this.count, bidderId);
            this.bid.setCreateDate(new Timestamp(itemInfo.getStartDate().getTime() + this.currentCreateDateAdvanceStep));
            this.bid.setUpdateDate(this.bid.getCreateDate());

            if (remaining == 0) {
                this.bid.setMaxBid(itemInfo.getCurrentPrice());
            } else {
                this.bid.setMaxBid(last_bid + this.currentBidPriceAdvanceStep);
            }

            // IB_ID
            row[col++] = this.bid.getId();
            // IB_I_ID
            row[col++] = itemInfo.getItemId();
            // IB_U_ID
            row[col++] = itemInfo.getSellerId();
            // IB_BUYER_ID
            row[col++] = this.bid.getBidderId();
            // IB_BID
            row[col++] = this.bid.getMaxBid() - (remaining > 0 ? (this.currentBidPriceAdvanceStep / 2.0f) : 0);
//            row[col++] = this.currentPrice;
            // IB_MAX_BID
            row[col++] = this.bid.getMaxBid();
            // IB_CREATED
            row[col++] = this.bid.getCreateDate();
            // IB_UPDATED
            row[col++] = this.bid.getUpdateDate();

            if (remaining == 0) {
                this.updateSubTableGenerators(itemInfo);
            }
            return (col);
        }

        @Override
        protected void newElementCallback(LoaderItemInfo itemInfo) {
            this.new_item = true;
            this.bid = null;
        }
    }

    /**********************************************************************************************
     * ITEM_BID_MAX Generator
     **********************************************************************************************/
    protected class ItemMaxBidGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemMaxBidGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID, AuctionMarkConstants.TABLENAME_ITEM_BID);
        }

        @Override
        public int getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.getBidCount() > 0 ? 1 : 0;
        }

        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, int remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();


            // IMB_I_ID
            row[col++] = itemInfo.getItemId();
            // IMB_U_ID
            row[col++] = itemInfo.getSellerId();
            // IMB_IB_ID
            row[col++] = bid.getId();
            // IMB_IB_I_ID
            row[col++] = itemInfo.getItemId();
            // IMB_IB_U_ID
            row[col++] = itemInfo.getSellerId();
            // IMB_CREATED
            row[col++] = bid.getCreateDate();
            // IMB_UPDATED
            row[col++] = bid.getUpdateDate();

            return (col);
        }
    }

    /**********************************************************************************************
     * ITEM_PURCHASE Generator
     **********************************************************************************************/
    protected class ItemPurchaseGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemPurchaseGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE, AuctionMarkConstants.TABLENAME_ITEM_BID);
        }

        @Override
        public int getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.getBidCount() > 0 && itemInfo.getPurchaseDate() != null ? 1 : 0;
        }

        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, int remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();


            // IP_ID
            row[col++] = this.count;
            // IP_IB_ID
            row[col++] = bid.getId();
            // IP_IB_I_ID
            row[col++] = itemInfo.getItemId();
            // IP_IB_U_ID
            row[col++] = itemInfo.getSellerId();
            // IP_DATE
            row[col++] = itemInfo.getPurchaseDate();

            if (profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_BUYER_LEAVES_FEEDBACK) {
                bid.setBuyer_feedback(true);
            }
            if (profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_SELLER_LEAVES_FEEDBACK) {
                bid.setSeller_feedback(true);
            }

            if (remaining == 0) {
                this.updateSubTableGenerators(bid);
            }
            return (col);
        }
    }

    /**********************************************************************************************
     * USER_FEEDBACK Generator
     **********************************************************************************************/
    protected class UserFeedbackGenerator extends SubTableGenerator<LoaderItemInfo.Bid> {

        public UserFeedbackGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_USERACCT_FEEDBACK, AuctionMarkConstants.TABLENAME_ITEM_PURCHASE);
        }

        @Override
        protected int getElementCounter(LoaderItemInfo.Bid bid) {
            return (bid.isBuyer_feedback() ? 1 : 0) + (bid.isSeller_feedback() ? 1 : 0);
        }

        @Override
        protected int populateRow(LoaderItemInfo.Bid bid, Object[] row, int remaining) {
            int col = 0;

            boolean is_buyer = false;
            is_buyer = remaining == 1 || (bid.isBuyer_feedback() && !bid.isSeller_feedback());
            LoaderItemInfo itemInfo = bid.getLoaderItemInfo();

            // UF_U_ID
            row[col++] = (is_buyer ? bid.getBidderId() : itemInfo.getSellerId());
            // UF_I_ID
            row[col++] = itemInfo.getItemId();
            // UF_I_U_ID
            row[col++] = itemInfo.getSellerId();
            // UF_FROM_ID
            row[col++] = (is_buyer ? itemInfo.getSellerId() : bid.getBidderId());
            // UF_RATING
            row[col++] = 1; // TODO
            // UF_DATE
            row[col++] = profile.getLoaderStartTime(); // Does this matter?

            return (col);
        }
    }

    /**********************************************************************************************
     * USER_ITEM Generator
     **********************************************************************************************/
    protected class UserItemGenerator extends SubTableGenerator<LoaderItemInfo> {

        public UserItemGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_USERACCT_ITEM, AuctionMarkConstants.TABLENAME_ITEM_BID);
        }

        @Override
        public int getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.getBidCount() > 0 && itemInfo.getPurchaseDate() != null ? 1 : 0;
        }

        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, int remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();


            // UI_U_ID
            row[col++] = bid.getBidderId();
            // UI_I_ID
            row[col++] = itemInfo.getItemId();
            // UI_I_U_ID
            row[col++] = itemInfo.getSellerId();
            // UI_IP_ID
            row[col++] = null;
            // UI_IP_IB_ID
            row[col++] = null;
            // UI_IP_IB_I_ID
            row[col++] = null;
            // UI_IP_IB_U_ID
            row[col++] = null;
            // UI_CREATED
            row[col++] = itemInfo.getEndDate();

            return (col);
        }
    }

    /**********************************************************************************************
     * USER_WATCH Generator
     **********************************************************************************************/
    protected class UserWatchGenerator extends SubTableGenerator<LoaderItemInfo> {

        final Set<UserId> watchers = new HashSet<>();

        public UserWatchGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_USERACCT_WATCH, AuctionMarkConstants.TABLENAME_ITEM_BID);
        }

        @Override
        public int getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.getNumWatches();
        }

        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, int remaining) {
            int col = 0;

            // Make it more likely that a user that has bid on an item is watching it
            Histogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
            UserId buyerId = null;
            int num_watchers = this.watchers.size();
            boolean use_random = (num_watchers == bidderHistogram.getValueCount());
            long num_users = tableSizes.get(AuctionMarkConstants.TABLENAME_USERACCT);

            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("Selecting USER_WATCH buyerId [useRandom=%s, watchers=%d]", use_random, this.watchers.size()));
            }
            int tries = 1000;
            while (num_watchers < num_users && tries-- > 0) {
                try {
                    if (use_random) {
                        buyerId = profile.getRandomBuyerId();
                    } else {
                        buyerId = profile.getRandomBuyerId(bidderHistogram, itemInfo.getSellerId());
                    }
                } catch (NullPointerException ex) {
                    LOG.error("Busted Bidder Histogram:\n{}", bidderHistogram);
                    throw ex;
                }
                if (!this.watchers.contains(buyerId)) {
                    break;
                }
                buyerId = null;

                // If for some reason we unable to find a buyer from our bidderHistogram,
                // then just give up and get a random one
                if (!use_random && tries == 0) {
                    use_random = true;
                    tries = 500;
                }
            }
            this.watchers.add(buyerId);

            // UW_U_ID
            row[col++] = buyerId;
            // UW_I_ID
            row[col++] = itemInfo.getItemId();
            // UW_I_U_ID
            row[col++] = itemInfo.getSellerId();
            // UW_CREATED
            row[col++] = this.getRandomDate(itemInfo.getStartDate(), itemInfo.getEndDate());

            return (col);
        }

        @Override
        protected void finishElementCallback(LoaderItemInfo t) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Clearing watcher cache [size={}]", this.watchers.size());
            }
            this.watchers.clear();
        }

        private Timestamp getRandomDate(Timestamp startDate, Timestamp endDate) {
            int start = Math.round(startDate.getTime() / AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
            int end = Math.round(endDate.getTime() / AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
            long offset = profile.rng.number(start, end);
            return new Timestamp(offset * AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
        }
    }
}