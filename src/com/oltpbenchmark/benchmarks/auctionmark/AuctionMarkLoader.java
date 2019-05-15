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

import java.io.File;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.auctionmark.util.Category;
import com.oltpbenchmark.benchmarks.auctionmark.util.CategoryParser;
import com.oltpbenchmark.benchmarks.auctionmark.util.GlobalAttributeGroupId;
import com.oltpbenchmark.benchmarks.auctionmark.util.GlobalAttributeValueId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.util.LoaderItemInfo;
import com.oltpbenchmark.benchmarks.auctionmark.util.UserId;
import com.oltpbenchmark.benchmarks.auctionmark.util.UserIdGenerator;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.CollectionUtil;
import com.oltpbenchmark.util.CompositeId;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.Pair;
import com.oltpbenchmark.util.RandomDistribution.Flat;
import com.oltpbenchmark.util.RandomDistribution.Zipf;
import com.oltpbenchmark.util.SQLUtil;

/**
 * @author pavlo
 * @author visawee
 */
public class AuctionMarkLoader extends Loader<AuctionMarkBenchmark> {
    private static final Logger LOG = Logger.getLogger(AuctionMarkLoader.class);

    // -----------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // -----------------------------------------------------------------

    protected final AuctionMarkProfile profile;

    /**
     * Data Generator Classes TableName -> AbstactTableGenerator
     */
    private final Map<String, AbstractTableGenerator> generators = Collections.synchronizedMap(new ListOrderedMap<String, AbstractTableGenerator>());

    private final Collection<String> sub_generators = new HashSet<String>();

    /** The set of tables that we have finished loading **/
    private final transient Collection<String> finished = Collections.synchronizedCollection(new HashSet<String>());

    private final Histogram<String> tableSizes = new Histogram<String>();

    private final File category_file;

    private boolean fail = false;

    // -----------------------------------------------------------------
    // INITIALIZATION
    // -----------------------------------------------------------------

    /**
     * Constructor
     * 
     * @param args
     */
    public AuctionMarkLoader(AuctionMarkBenchmark benchmark) {
        super(benchmark);

        // BenchmarkProfile
        this.profile = new AuctionMarkProfile(benchmark, benchmark.getRandomGenerator());

        this.category_file = new File(benchmark.getDataDir().getAbsolutePath() + "/table.category.gz");

        try {
            // ---------------------------
            // Fixed-Size Table Generators
            // ---------------------------

            this.registerGenerator(new RegionGenerator());
            this.registerGenerator(new CategoryGenerator(category_file));
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
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    // -----------------------------------------------------------------
    // LOADING METHODS
    // -----------------------------------------------------------------
    private class CountdownLoaderThread extends LoaderThread {
        private final AbstractTableGenerator generator;
        private final CountDownLatch latch;

        public CountdownLoaderThread(AbstractTableGenerator generator, CountDownLatch latch) throws SQLException {
            this.generator = generator;
            this.latch = latch;
        }

        @Override
        public void load(Connection conn) throws SQLException {
            LOG.debug(String.format("Started loading %s which depends on %s", this.generator.getTableName(), this.generator.getDependencies()));
            this.generator.load(conn);
            this.latch.countDown();
            LOG.debug(String.format("Finished loading %s", this.generator.getTableName()));
        }
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();

        final CountDownLatch loadLatch = new CountDownLatch(this.generators.size());

        for (AbstractTableGenerator generator : this.generators.values()) {
            generator.init();
            threads.add(new CountdownLoaderThread(generator, loadLatch));
        }

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    loadLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                AuctionMarkLoader.this.profile.saveProfile(conn);
            }
        });

        return threads;
    }

    private void registerGenerator(AbstractTableGenerator generator) {
        // Register this one as well as any sub-generators
        this.generators.put(generator.getTableName(), generator);
        for (AbstractTableGenerator sub_generator : generator.getSubTableGenerators()) {
            this.registerGenerator(sub_generator);
            this.sub_generators.add(sub_generator.getTableName());
        } // FOR
    }
    
    protected AbstractTableGenerator getGenerator(String table_name) {
        return (this.generators.get(table_name));
    }

    /**
     * Load the tuples for the given table name
     * @param tableName
     */
    protected void generateTableData(Connection conn, String tableName) throws SQLException {
        LOG.info("*** START " + tableName);
        final AbstractTableGenerator generator = this.generators.get(tableName);
        assert (generator != null);

        // Generate Data
        final Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
        assert(catalog_tbl != null) : tableName;
        final List<Object[]> volt_table = generator.getVoltTable();
        final String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        final PreparedStatement stmt = conn.prepareStatement(sql);
        final int types[] = catalog_tbl.getColumnTypes();
        
        while (generator.hasMore()) {
            generator.generateBatch();
            
//            StringBuilder sb = new StringBuilder();
//            if (tableName.equalsIgnoreCase("USER_FEEDBACK")) { //  || tableName.equalsIgnoreCase("USER_ATTRIBUTES")) {
//                sb.append(tableName + "\n");
//                for (int i = 0; i < volt_table.size(); i++) {
//                    sb.append(String.format("[%03d] %s\n", i, StringUtil.abbrv(Arrays.toString(volt_table.get(i)), 100)));
//                }
//                LOG.info(sb.toString() + "\n");
//            }
            
            for (Object row[] : volt_table) {
                for (int i = 0; i < row.length; i++) {
                    if (row[i] != null) {
                        stmt.setObject(i+1, row[i]);
                    } else {
                        stmt.setNull(i+1, types[i]);
                    }
                } // FOR
                stmt.addBatch();
            } // FOR
            try {
                stmt.executeBatch();
                conn.commit();
                stmt.clearBatch();
            } catch (SQLException ex) {
                if (ex.getNextException() != null) ex = ex.getNextException();
                LOG.warn(tableName + " - " + ex.getMessage());
                throw ex;
                // SKIP
            }
            
            this.tableSizes.put(tableName, volt_table.size());
            
            // Release anything to the sub-generators if we have it
            // We have to do this to ensure that all of the parent tuples get
            // insert first for foreign-key relationships
            generator.releaseHoldsToSubTableGenerators();
        } // WHILE
        stmt.close();
        
        // Mark as finished
        if (this.fail == false) {
            generator.markAsFinished();
            synchronized (this) {
                this.finished.add(tableName);
                LOG.info(String.format("*** FINISH %s - %d tuples - [%d / %d]",
                                       tableName, this.tableSizes.get(tableName),
                                       this.finished.size(), this.generators.size()));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Remaining Tables: " + CollectionUtils.subtract(this.generators.keySet(), this.finished));
                }
            } // SYNCH
        }
    }

    /**********************************************************************************************
     * AbstractTableGenerator
     **********************************************************************************************/
    protected abstract class AbstractTableGenerator extends LoaderThread {
        private final String tableName;
        private final Table catalog_tbl;
        protected final List<Object[]> table = new ArrayList<Object[]>();
        protected Long tableSize;
        protected Long batchSize;
        protected final CountDownLatch latch = new CountDownLatch(1);
        protected final List<String> dependencyTables = new ArrayList<String>();

        /**
         * Some generators have children tables that we want to load tuples for each batch of this generator. 
         * The queues we need to update every time we generate a new LoaderItemInfo
         */
        protected final Set<SubTableGenerator<?>> sub_generators = new HashSet<SubTableGenerator<?>>();  

        protected final List<Object> subGenerator_hold = new ArrayList<Object>();
        
        protected long count = 0;
        
        /** Any column with the name XX_SATTR## will automatically be filled with a random string */
        protected final List<Column> random_str_cols = new ArrayList<Column>();
        protected final Pattern random_str_regex = Pattern.compile("[\\w]+\\_SATTR[\\d]+", Pattern.CASE_INSENSITIVE);
        
        /** Any column with the name XX_IATTR## will automatically be filled with a random integer */
        protected List<Column> random_int_cols = new ArrayList<Column>();
        protected final Pattern random_int_regex = Pattern.compile("[\\w]+\\_IATTR[\\d]+", Pattern.CASE_INSENSITIVE);

        /**
         * Constructor
         * @param catalog_tbl
         */
        public AbstractTableGenerator(String tableName, String...dependencies) throws SQLException {
            super();
            this.tableName = tableName;
            this.catalog_tbl = benchmark.getCatalog().getTable(tableName);
            assert(catalog_tbl != null) : "Invalid table name '" + tableName + "'";
            
            boolean fixed_size = AuctionMarkConstants.FIXED_TABLES.contains(catalog_tbl.getName());
            boolean dynamic_size = AuctionMarkConstants.DYNAMIC_TABLES.contains(catalog_tbl.getName());
            boolean data_file = AuctionMarkConstants.DATAFILE_TABLES.contains(catalog_tbl.getName());

            // Add the dependencies so that we know what we need to block on
            CollectionUtil.addAll(this.dependencyTables, dependencies);
            
            String field_name = "BATCHSIZE_" + catalog_tbl.getName();
            try {
                Field field_handle = AuctionMarkConstants.class.getField(field_name);
                assert (field_handle != null);
                this.batchSize = (Long) field_handle.get(null);
            } catch (Exception ex) {
                throw new RuntimeException("Missing field '" + field_name + "' needed for '" + tableName + "'", ex);
            } 

            // Initialize dynamic parameters for tables that are not loaded from data files
            if (!data_file && !dynamic_size && tableName.equalsIgnoreCase(AuctionMarkConstants.TABLENAME_ITEM) == false) {
                field_name = "TABLESIZE_" + catalog_tbl.getName();
                try {
                    
                    Field field_handle = AuctionMarkConstants.class.getField(field_name);
                    assert (field_handle != null);
                    this.tableSize = (Long) field_handle.get(null);
                    if (!fixed_size) {
                        this.tableSize = (long)Math.max(1, (int)Math.round(this.tableSize * profile.getScaleFactor()));
                    }
                } catch (NoSuchFieldException ex) {
                    if (LOG.isDebugEnabled()) LOG.warn("No table size field for '" + tableName + "'", ex);
                } catch (Exception ex) {
                    throw new RuntimeException("Missing field '" + field_name + "' needed for '" + tableName + "'", ex);
                } 
            } 
            
            for (Column catalog_col : this.catalog_tbl.getColumns()) {
                if (random_str_regex.matcher(catalog_col.getName()).matches()) {
                    assert(SQLUtil.isStringType(catalog_col.getType())) : catalog_col.fullName();
                    this.random_str_cols.add(catalog_col);
                    if (LOG.isTraceEnabled()) LOG.trace("Random String Column: " + catalog_col.fullName());
                }
                else if (random_int_regex.matcher(catalog_col.getName()).matches()) {
                    assert(SQLUtil.isIntegerType(catalog_col.getType())) : catalog_col.fullName();
                    this.random_int_cols.add(catalog_col);
                    if (LOG.isTraceEnabled()) LOG.trace("Random Integer Column: " + catalog_col.fullName());
                }
            } // FOR
            if (LOG.isDebugEnabled()) {
                if (this.random_str_cols.size() > 0) LOG.debug(String.format("%s Random String Columns: %s", tableName, this.random_str_cols));
                if (this.random_int_cols.size() > 0) LOG.debug(String.format("%s Random Integer Columns: %s", tableName, this.random_int_cols));
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
         * @param row TODO
         */
        protected abstract int populateRow(Object[] row);

        @Override
        public void load(Connection conn) throws SQLException {
            // First block on the CountDownLatches of all the tables that we depend on
            if (this.dependencyTables.size() > 0 && LOG.isDebugEnabled())
                LOG.debug(String.format("%s: Table generator is blocked waiting for %d other tables: %s",
                                        this.tableName, this.dependencyTables.size(), this.dependencyTables));
            for (String dependency : this.dependencyTables) {
                AbstractTableGenerator gen = AuctionMarkLoader.this.generators.get(dependency);
                assert(gen != null) : "Missing table generator for '" + dependency + "'";
                try {
                    gen.latch.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException("Unexpected interruption for '" + this.tableName + "' waiting for '" + dependency + "'", ex);
                }
            } // FOR
            
            // Make sure we call prepare before we start generating table data
            this.prepare();
            
            // Then invoke the loader generation method
            try {
                AuctionMarkLoader.this.generateTableData(conn, this.tableName);
            } catch (Throwable ex) {
                ex.printStackTrace();
                throw new RuntimeException("Unexpected error while generating table data for '" + this.tableName + "'", ex);
            }
        }
        
        @SuppressWarnings("unchecked")
        public <T extends AbstractTableGenerator> T addSubTableGenerator(SubTableGenerator<?> sub_item) {
            this.sub_generators.add(sub_item);
            return ((T)this);
        }
        @SuppressWarnings("unchecked")
        public void releaseHoldsToSubTableGenerators() {
            if (this.subGenerator_hold.isEmpty() == false) {
                LOG.debug(String.format("%s: Releasing %d held objects to %d sub-generators",
                                        this.tableName, this.subGenerator_hold.size(), this.sub_generators.size()));
                for (@SuppressWarnings("rawtypes") SubTableGenerator sub_generator : this.sub_generators) {
                    sub_generator.queue.addAll(this.subGenerator_hold);
                } // FOR
                this.subGenerator_hold.clear();
            }
        }
        public void updateSubTableGenerators(Object obj) {
            // Queue up this item for our multi-threaded sub-generators
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("%s: Updating %d sub-generators with %s: %s",
                                        this.tableName, this.sub_generators.size(), obj, this.sub_generators));
            this.subGenerator_hold.add(obj);
        }
        public boolean hasSubTableGenerators() {
            return (!this.sub_generators.isEmpty());
        }
        public Collection<SubTableGenerator<?>> getSubTableGenerators() {
            return (this.sub_generators);
        }
        public Collection<String> getSubGeneratorTableNames() {
            List<String> names = new ArrayList<String>();
            for (AbstractTableGenerator gen : this.sub_generators) {
                names.add(gen.catalog_tbl.getName());
            }
            return (names);
        }
        
        protected int populateRandomColumns(Object row[]) {
            int cols = 0;
            
            // STRINGS
            for (Column catalog_col : this.random_str_cols) {
                int size = catalog_col.getSize();
                row[catalog_col.getIndex()] = profile.rng.astring(profile.rng.nextInt(size - 1), size);
                cols++;
            } // FOR
            
            // INTEGER
            for (Column catalog_col : this.random_int_cols) {
                row[catalog_col.getIndex()] = profile.rng.number(0, 1<<30);
                cols++;
            } // FOR
            
            return (cols);
        }

        /**
         * Returns true if this generator has more tuples that it wants to add
         * @return
         */
        public synchronized boolean hasMore() {
            return (this.count < this.tableSize);
        }
        /**
         * Return the table's catalog object for this generator
         * @return
         */
        public Table getTableCatalog() {
            return (this.catalog_tbl);
        }
        /**
         * Return the VoltTable handle
         * @return
         */
        public List<Object[]> getVoltTable() {
            return this.table;
        }
        /**
         * Returns the number of tuples that will be loaded into this table
         * @return
         */
        public Long getTableSize() {
            return this.tableSize;
        }
        /**
         * Returns the number of tuples per batch that this generator will want loaded
         * @return
         */
        public Long getBatchSize() {
            return this.batchSize;
        }
        /**
         * Returns the name of the table this this generates
         * @return
         */
        public String getTableName() {
            return this.tableName;
        }
        /**
         * Returns the total number of tuples generated thusfar
         * @return
         */
        public synchronized long getCount() {
            return this.count;
        }

        /**
         * When called, the generator will populate a new row record and append it to the underlying VoltTable
         */
        public synchronized void addRow() {
            Object row[] = new Object[this.catalog_tbl.getColumnCount()];
            
            // Main Columns
            int cols = this.populateRow(row);
            
            // RANDOM COLS
            cols += this.populateRandomColumns(row);
            
            assert(cols == this.catalog_tbl.getColumnCount()) : 
                String.format("Invalid number of columns for %s [expected=%d, actual=%d]",
                              this.tableName, this.catalog_tbl.getColumnCount(), cols);
            
            // Convert all CompositeIds into their long encodings
            for (int i = 0; i < cols; i++) {
                if (row[i] != null && row[i] instanceof CompositeId) {
                    row[i] = ((CompositeId)row[i]).encode();
                }
            } // FOR
            
            this.count++;
            this.table.add(row);
        }
        /**
         * 
         */
        public void generateBatch() {
            if (LOG.isTraceEnabled()) LOG.trace(String.format("%s: Generating new batch", this.getTableName()));
            long batch_count = 0;
            this.table.clear();
            while (this.hasMore() && this.table.size() < this.batchSize) {
                this.addRow();
                batch_count++;
            } // WHILE
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("%s: Finished generating new batch of %d tuples", this.getTableName(), batch_count));
        }

        public void markAsFinished() {
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("%s: Marking as finished", this.tableName));
        	this.latch.countDown();
            for (SubTableGenerator<?> sub_generator : this.sub_generators) {
                sub_generator.stopWhenEmpty();
            } // FOR
        }
        
        public boolean isFinish(){
        	return (this.latch.getCount() == 0);
        }
        
        public List<String> getDependencies() {
            return this.dependencyTables;
        }
        
        @Override
        public String toString() {
            return String.format("Generator[%s]", this.tableName);
        }
    } // END CLASS

    /**********************************************************************************************
     * SubUserTableGenerator
     * This is for tables that are based off of the USER table
     **********************************************************************************************/
    protected abstract class SubTableGenerator<T> extends AbstractTableGenerator {
        
        private final LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<T>();
        private T current;
        private short currentCounter;
        private boolean stop = false;
        private final String sourceTableName;

        public SubTableGenerator(String tableName, String sourceTableName, String...dependencies) throws SQLException {
            super(tableName, dependencies);
            this.sourceTableName = sourceTableName;
        }
        
        protected abstract short getElementCounter(T t);
        protected abstract int populateRow(T t, Object[] row, short remaining);
        
        public void stopWhenEmpty() {
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("%s: Will stop when queue is empty", this.getTableName()));
            this.stop = true;
        }
        
        @Override
        public void init() {
            // Get the AbstractTableGenerator that will feed into this generator
            AbstractTableGenerator parent_gen = AuctionMarkLoader.this.generators.get(this.sourceTableName);
            assert(parent_gen != null) : "Unexpected source TableGenerator '" + this.sourceTableName + "'";
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
            assert(t != null);
            this.currentCounter--;
            return (this.populateRow(t, row, this.currentCounter));
        }
        private final T getNext() {
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
                        if (this.stop) break;
                        continue;
                    }
                    this.currentCounter = this.getElementCounter(this.current);
                } // WHILE
            }
            if (last != this.current) {
                if (last != null) this.finishElementCallback(last);
                if (this.current != null) this.newElementCallback(this.current);
            }
            return this.current;
        }
        protected void finishElementCallback(T t) {
            // Nothing...
        }
        protected void newElementCallback(T t) {
            // Nothing... 
        }
    } // END CLASS
    
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
            row[col++] = Integer.valueOf((int) this.count);
            // R_NAME
            row[col++] = profile.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * CATEGORY Generator
     **********************************************************************************************/
    protected class CategoryGenerator extends AbstractTableGenerator {
        private final File data_file;
        private final Map<String, Category> categoryMap;
        private final LinkedList<Category> categories = new LinkedList<Category>();

        public CategoryGenerator(File data_file) throws SQLException {
            super(AuctionMarkConstants.TABLENAME_CATEGORY);
            this.data_file = data_file;
            assert(this.data_file.exists()) : 
                "The data file for the category generator does not exist: " + this.data_file;

            this.categoryMap = (new CategoryParser(data_file)).getCategoryMap();
            this.tableSize = (long)this.categoryMap.size();
        }

        @Override
        public void init() {
            for (Category category : this.categoryMap.values()) {
                if (category.isLeaf()) {
                    profile.items_per_category.put(category.getCategoryID(), category.getItemCount());
                }
                this.categories.add(category);
            } // FOR
        }
        @Override
        public void prepare() {
            // Nothing to do
        }
        @Override
        protected int populateRow(Object[] row) {
            int col = 0;

            Category category = this.categories.poll();
            assert(category != null);
            
            // C_ID
            row[col++] = category.getCategoryID();
            // C_NAME
            row[col++] = category.getName();
            // C_PARENT_ID
            row[col++] = category.getParentCategoryID();
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_GROUP Generator
     **********************************************************************************************/
    protected class GlobalAttributeGroupGenerator extends AbstractTableGenerator {
        private long num_categories = 0l;
        private final Histogram<Integer> category_groups = new Histogram<Integer>();
        private final LinkedList<GlobalAttributeGroupId> group_ids = new LinkedList<GlobalAttributeGroupId>();

        public GlobalAttributeGroupGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP,
                  AuctionMarkConstants.TABLENAME_CATEGORY);
        }

        @Override
        public void init() {
            // Nothing to do
        }
        @Override
        public void prepare() {
            // Grab the number of CATEGORY items that we have inserted
            this.num_categories = getGenerator(AuctionMarkConstants.TABLENAME_CATEGORY).tableSize;
            
            for (int i = 0; i < this.tableSize; i++) {
                int category_id = profile.rng.number(0, (int)this.num_categories);
                this.category_groups.put(category_id);
                int id = this.category_groups.get(category_id).intValue();
                int count = (int)profile.rng.number(1, AuctionMarkConstants.TABLESIZE_GLOBAL_ATTRIBUTE_VALUE_PER_GROUP);
                GlobalAttributeGroupId gag_id = new GlobalAttributeGroupId(category_id, id, count);
                assert(profile.gag_ids.contains(gag_id) == false);
                profile.gag_ids.add(gag_id);
                this.group_ids.add(gag_id);
            } // FOR
        }
        @Override
        protected int populateRow(Object[] row) {
            int col = 0;

            GlobalAttributeGroupId gag_id = this.group_ids.poll();
            assert(gag_id != null);
            
            // GAG_ID
            row[col++] = gag_id.encode();
            // GAG_C_ID
            row[col++] = gag_id.getCategoryId();
            // GAG_NAME
            row[col++] = profile.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_VALUE Generator
     **********************************************************************************************/
    protected class GlobalAttributeValueGenerator extends AbstractTableGenerator {

        private Histogram<GlobalAttributeGroupId> gag_counters = new Histogram<GlobalAttributeGroupId>(true);
        private Iterator<GlobalAttributeGroupId> gag_iterator;
        private GlobalAttributeGroupId gag_current;
        private int gav_counter = -1;

        public GlobalAttributeValueGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE,
                  AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
        }

        @Override
        public void init() {
            // Nothing to do
        }
        @Override
        public void prepare() {
            this.tableSize = 0l;
            for (GlobalAttributeGroupId gag_id : profile.gag_ids) {
                this.gag_counters.set(gag_id, 0);
                this.tableSize += gag_id.getCount();
            } // FOR
            this.gag_iterator = profile.gag_ids.iterator();
        }
        @Override
        protected int populateRow(Object[] row) {
            int col = 0;
            
            if (this.gav_counter == -1 || ++this.gav_counter == this.gag_current.getCount()) {
                this.gag_current = this.gag_iterator.next();
                assert(this.gag_current != null);
                this.gav_counter = 0;
            }

            GlobalAttributeValueId gav_id = new GlobalAttributeValueId(this.gag_current.encode(),
                                                                     this.gav_counter);
            
            // GAV_ID
            row[col++] = gav_id.encode();
            // GAV_GAG_ID
            row[col++] = this.gag_current.encode();
            // GAV_NAME
            row[col++] = profile.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * USER Generator
     **********************************************************************************************/
    protected class UserGenerator extends AbstractTableGenerator {
        private final Zipf randomBalance;
        private final Flat randomRegion;
        private final Zipf randomRating;
        private UserIdGenerator idGenerator;
        
        public UserGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_USERACCT,
                  AuctionMarkConstants.TABLENAME_REGION);
            this.randomRegion = new Flat(profile.rng, 0, (int)AuctionMarkConstants.TABLESIZE_REGION);
            this.randomRating = new Zipf(profile.rng, AuctionMarkConstants.USER_MIN_RATING,
                                                      AuctionMarkConstants.USER_MAX_RATING, 1.0001);
            this.randomBalance = new Zipf(profile.rng, AuctionMarkConstants.USER_MIN_BALANCE,
                                                       AuctionMarkConstants.USER_MAX_BALANCE, 1.001);
        }

        @Override
        public void init() {
            // Populate the profile's users per item count histogram so that we know how many
            // items that each user should have. This will then be used to calculate the
            // the user ids by placing them into numeric ranges
            int max_items = Math.max(1, (int)Math.ceil(AuctionMarkConstants.ITEM_ITEMS_PER_SELLER_MAX * profile.getScaleFactor()));
            assert(max_items > 0);
            LOG.debug("Max Items Per Seller: " + max_items);
            Zipf randomNumItems = new Zipf(profile.rng,
                       AuctionMarkConstants.ITEM_ITEMS_PER_SELLER_MIN,
                       max_items,
                       AuctionMarkConstants.ITEM_ITEMS_PER_SELLER_SIGMA);
            for (long i = 0; i < this.tableSize; i++) {
                long num_items = randomNumItems.nextInt();
                profile.users_per_itemCount.put(num_items);
            } // FOR
             if (LOG.isDebugEnabled())
                LOG.debug("Users Per Item Count:\n" + profile.users_per_itemCount);
            this.idGenerator = new UserIdGenerator(profile.users_per_itemCount, benchmark.getWorkloadConfiguration().getTerminals());
            assert(this.idGenerator.hasNext());
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
            super(AuctionMarkConstants.TABLENAME_USERACCT_ATTRIBUTES,
                  AuctionMarkConstants.TABLENAME_USERACCT);
            
            this.randomNumUserAttributes = new Zipf(profile.rng,
                                                    AuctionMarkConstants.USER_MIN_ATTRIBUTES,
                                                    AuctionMarkConstants.USER_MAX_ATTRIBUTES, 1.001);
        }
        @Override
        protected short getElementCounter(UserId user_id) {
            return (short)(randomNumUserAttributes.nextInt());
        }
        @Override
        protected int populateRow(UserId user_id, Object[] row, short remaining) {
            int col = 0;
            
            // UA_ID
            row[col++] = this.count;
            // UA_U_ID
            row[col++] = user_id;
            // UA_NAME
            row[col++] = profile.rng.astring(AuctionMarkConstants.USER_ATTRIBUTE_NAME_LENGTH_MIN,
                                             AuctionMarkConstants.USER_ATTRIBUTE_NAME_LENGTH_MAX);
            // UA_VALUE
            row[col++] = profile.rng.astring(AuctionMarkConstants.USER_ATTRIBUTE_VALUE_LENGTH_MIN,
                                             AuctionMarkConstants.USER_ATTRIBUTE_VALUE_LENGTH_MAX);
            // U_CREATED
            row[col++] = new Timestamp(System.currentTimeMillis());
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * ITEM Generator
     **********************************************************************************************/
    protected class ItemGenerator extends SubTableGenerator<UserId> {
        
        /**
         * BidDurationDay -> Pair<NumberOfBids, NumberOfWatches>
         */
        private final Map<Long, Pair<Zipf, Zipf>> item_bid_watch_zipfs = new HashMap<Long, Pair<Zipf,Zipf>>();
        
        public ItemGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_ITEM,
                  AuctionMarkConstants.TABLENAME_USERACCT,
                  AuctionMarkConstants.TABLENAME_USERACCT,
                  AuctionMarkConstants.TABLENAME_CATEGORY);
        }
        
        @Override
        protected short getElementCounter(UserId user_id) {
            return (short)(user_id.getItemCount());
        }

        @Override
        public void init() {
            super.init();
            this.tableSize = 0l;
            for (Long size : profile.users_per_itemCount.values()) {
                this.tableSize += size.intValue() * profile.users_per_itemCount.get(size);
            } // FOR
        }
        @Override
        protected int populateRow(UserId seller_id, Object[] row, short remaining) {
            int col = 0;
            
            ItemId itemId = new ItemId(seller_id, remaining);
            Timestamp endDate = this.getRandomEndTimestamp();
            Timestamp startDate = this.getRandomStartTimestamp(endDate); 
            if (LOG.isTraceEnabled())
                LOG.trace("endDate = " + endDate + " : startDate = " + startDate);
            
            long bidDurationDay = ((endDate.getTime() - startDate.getTime()) / AuctionMarkConstants.MILLISECONDS_IN_A_DAY);
            Pair<Zipf, Zipf> p = this.item_bid_watch_zipfs.get(bidDurationDay);
            if (p == null) {
                Zipf randomNumBids = new Zipf(profile.rng,
                        AuctionMarkConstants.ITEM_BIDS_PER_DAY_MIN * (int)bidDurationDay,
                        AuctionMarkConstants.ITEM_BIDS_PER_DAY_MAX * (int)bidDurationDay,
                        AuctionMarkConstants.ITEM_BIDS_PER_DAY_SIGMA);
                Zipf randomNumWatches = new Zipf(profile.rng,
                        AuctionMarkConstants.ITEM_WATCHES_PER_DAY_MIN * (int)bidDurationDay,
                        AuctionMarkConstants.ITEM_WATCHES_PER_DAY_MAX * (int)bidDurationDay,
                        AuctionMarkConstants.ITEM_WATCHES_PER_DAY_SIGMA);
                p = Pair.of(randomNumBids, randomNumWatches);
                this.item_bid_watch_zipfs.put(bidDurationDay, p);
            }
            assert(p != null);

            // Calculate the number of bids and watches for this item
            short numBids = (short)p.first.nextInt();
            short numWatches = (short)p.second.nextInt();
            
            // Create the ItemInfo object that we will use to cache the local data 
            // for this item. This will get garbage collected once all the derivative
            // tables are done with it.
            LoaderItemInfo itemInfo = new LoaderItemInfo(itemId, endDate, numBids);
            itemInfo.sellerId = seller_id;
            itemInfo.startDate = startDate;
            itemInfo.initialPrice = profile.randomInitialPrice.nextInt();
            assert(itemInfo.initialPrice > 0) : "Invalid initial price for " + itemId;
            itemInfo.numImages = (short) profile.randomNumImages.nextInt();
            itemInfo.numAttributes = (short) profile.randomNumAttributes.nextInt();
            itemInfo.numBids = numBids;
            itemInfo.numWatches = numWatches;
            
            // The auction for this item has already closed
            if (itemInfo.endDate.getTime() <= profile.getLoaderStartTime().getTime()) {
                // Somebody won a bid and bought the item
                if (itemInfo.numBids > 0) {
                    itemInfo.lastBidderId = profile.getRandomBuyerId(itemInfo.sellerId);
                    itemInfo.purchaseDate = this.getRandomPurchaseTimestamp(itemInfo.endDate);
                    itemInfo.numComments = (short) profile.randomNumComments.nextInt();
                }
                itemInfo.status = ItemStatus.CLOSED;
            }
            // Item is still available
            else if (itemInfo.numBids > 0) {
        		itemInfo.lastBidderId = profile.getRandomBuyerId(itemInfo.sellerId);
            }
            profile.addItemToProperQueue(itemInfo, true);

            // I_ID
            row[col++] = itemInfo.itemId;
            // I_U_ID
            row[col++] = itemInfo.sellerId;
            // I_C_ID
            row[col++] = profile.getRandomCategoryId();
            // I_NAME
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_NAME_LENGTH_MIN,
                                             AuctionMarkConstants.ITEM_NAME_LENGTH_MAX);
            // I_DESCRIPTION
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_DESCRIPTION_LENGTH_MIN,
                                             AuctionMarkConstants.ITEM_DESCRIPTION_LENGTH_MAX);
            // I_USER_ATTRIBUTES
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_USER_ATTRIBUTES_LENGTH_MIN,
                                             AuctionMarkConstants.ITEM_USER_ATTRIBUTES_LENGTH_MAX);
            // I_INITIAL_PRICE
            row[col++] = itemInfo.initialPrice;

            // I_CURRENT_PRICE
            if (itemInfo.numBids > 0) {
                itemInfo.currentPrice = itemInfo.initialPrice + (itemInfo.numBids * itemInfo.initialPrice * AuctionMarkConstants.ITEM_BID_PERCENT_STEP);
                row[col++] = itemInfo.currentPrice;
            } else {
                row[col++] = itemInfo.initialPrice;
            }

            // I_NUM_BIDS
            row[col++] = itemInfo.numBids;
            // I_NUM_IMAGES
            row[col++] = itemInfo.numImages;
            // I_NUM_GLOBAL_ATTRS
            row[col++] = itemInfo.numAttributes;
            // I_NUM_COMMENTS
            row[col++] = itemInfo.numComments;
            // I_START_DATE
            row[col++] = itemInfo.startDate;
            // I_END_DATE
            row[col++] = itemInfo.endDate;
            // I_STATUS
            row[col++] = itemInfo.status.ordinal();
            // I_CREATED
            row[col++] = profile.getLoaderStartTime();
            // I_UPDATED
            row[col++] = itemInfo.startDate;

            this.updateSubTableGenerators(itemInfo);
            return (col);
        }

        private Timestamp getRandomStartTimestamp(Timestamp endDate) {
            long duration = ((long)profile.randomDuration.nextInt()) * AuctionMarkConstants.MILLISECONDS_IN_A_DAY;
            long lStartTimestamp = endDate.getTime() - duration;
            Timestamp startTimestamp = new Timestamp(lStartTimestamp);
            return startTimestamp;
        }
        private Timestamp getRandomEndTimestamp() {
            int timeDiff = profile.randomTimeDiff.nextInt();
            Timestamp time = new Timestamp(profile.getLoaderStartTime().getTime() + (timeDiff * AuctionMarkConstants.MILLISECONDS_IN_A_SECOND));
//            LOG.info(timeDiff + " => " + sdf.format(time.asApproximateJavaDate()));
            return time;
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
            super(AuctionMarkConstants.TABLENAME_ITEM_IMAGE,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.numImages;
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;

            // II_ID
            row[col++] = this.count;
            // II_I_ID
            row[col++] = itemInfo.itemId;
            // II_U_ID
            row[col++] = itemInfo.sellerId;

            return (col);
        }
    } // END CLASS
    
    /**********************************************************************************************
     * ITEM_ATTRIBUTE Generator
     **********************************************************************************************/
    protected class ItemAttributeGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemAttributeGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE,
                  AuctionMarkConstants.TABLENAME_ITEM,
                  AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP,
                  AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.numAttributes;
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            GlobalAttributeValueId gav_id = profile.getRandomGlobalAttributeValue();
            assert(gav_id != null);
            
            // IA_ID
            row[col++] = this.count;
            // IA_I_ID
            row[col++] = itemInfo.itemId;
            // IA_U_ID
            row[col++] = itemInfo.sellerId;
            // IA_GAV_ID
            row[col++] = gav_id.encode();
            // IA_GAG_ID
            row[col++] = gav_id.getGlobalAttributeGroup().encode();

            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * ITEM_COMMENT Generator
     **********************************************************************************************/
    protected class ItemCommentGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemCommentGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_ITEM_COMMENT,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (itemInfo.purchaseDate != null ? itemInfo.numComments : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;

            // IC_ID
            row[col++] = Integer.valueOf((int) this.count);
            // IC_I_ID
            row[col++] = itemInfo.itemId;
            // IC_U_ID
            row[col++] = itemInfo.sellerId;
            // IC_BUYER_ID
            row[col++] = itemInfo.lastBidderId;
            // IC_QUESTION
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_COMMENT_LENGTH_MIN,
                                             AuctionMarkConstants.ITEM_COMMENT_LENGTH_MAX);
            // IC_RESPONSE
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_COMMENT_LENGTH_MIN,
                                             AuctionMarkConstants.ITEM_COMMENT_LENGTH_MAX);
            // IC_CREATED
            row[col++] = this.getRandomCommentDate(itemInfo.startDate, itemInfo.endDate);
            // IC_UPDATED
            row[col++] = this.getRandomCommentDate(itemInfo.startDate, itemInfo.endDate);

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
            super(AuctionMarkConstants.TABLENAME_ITEM_BID,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return ((short)itemInfo.numBids);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            assert(itemInfo.numBids > 0);
            
            UserId bidderId = null;
            
            // Figure out the UserId for the person bidding on this item now
            if (this.new_item) {
                // If this is a new item and there is more than one bid, then
                // we'll choose the bidder's UserId at random.
                // If there is only one bid, then it will have to be the last bidder
                bidderId = (itemInfo.numBids == 1 ? itemInfo.lastBidderId :
                                                    profile.getRandomBuyerId(itemInfo.sellerId));
                Timestamp endDate;
                if (itemInfo.status == ItemStatus.OPEN) {
                    endDate = profile.getLoaderStartTime();
                } else {
                    endDate = itemInfo.endDate;
                }
                this.currentCreateDateAdvanceStep = (endDate.getTime() - itemInfo.startDate.getTime()) / (remaining + 1);
//                this.currentBidPriceAdvanceStep = (itemInfo.currentPrice - itemInfo.initialPrice) * itemInfo.numBids;
                this.currentBidPriceAdvanceStep = itemInfo.initialPrice * AuctionMarkConstants.ITEM_BID_PERCENT_STEP;
                this.currentPrice = itemInfo.initialPrice;
            }
            // The last bid must always be the item's lastBidderId
            else if (remaining == 0) {
                bidderId = itemInfo.lastBidderId;
                this.currentPrice = itemInfo.currentPrice;
            }
            // The first bid for a two-bid item must always be different than the lastBidderId
            else if (itemInfo.numBids == 2) {
                assert(remaining == 1);
                bidderId = profile.getRandomBuyerId(itemInfo.lastBidderId, itemInfo.sellerId);
            } 
            // Since there are multiple bids, we want randomly select one based on the previous bidders
            // We will get the histogram of bidders so that we are more likely to select
            // an existing bidder rather than a completely random one
            else {
                assert(this.bid != null);
                Histogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
                bidderId = profile.getRandomBuyerId(bidderHistogram, this.bid.bidderId, itemInfo.sellerId);
                this.currentPrice += this.currentBidPriceAdvanceStep;
            }
            assert(bidderId != null);

            float last_bid = (this.new_item ? itemInfo.initialPrice : this.bid.maxBid);
            this.bid = itemInfo.getNextBid(this.count, bidderId);
            this.bid.createDate = new Timestamp(itemInfo.startDate.getTime() + this.currentCreateDateAdvanceStep);
            this.bid.updateDate = this.bid.createDate; 
            
            if (remaining == 0) {
                this.bid.maxBid = itemInfo.currentPrice;
                if (itemInfo.purchaseDate != null) {
                    assert(itemInfo.getBidCount() == itemInfo.numBids) :
                        String.format("%d != %d\n%s", itemInfo.getBidCount(), itemInfo.numBids, itemInfo);
                }
            } else {
                this.bid.maxBid = last_bid + this.currentBidPriceAdvanceStep;
            }
            
            // IB_ID
            row[col++] = Long.valueOf(this.bid.id);
            // IB_I_ID
            row[col++] = itemInfo.itemId;
            // IB_U_ID
            row[col++] = itemInfo.sellerId;
            // IB_BUYER_ID
            row[col++] = this.bid.bidderId;
            // IB_BID
            row[col++] = this.bid.maxBid - (remaining > 0 ? (this.currentBidPriceAdvanceStep/2.0f) : 0);
//            row[col++] = this.currentPrice;   
            // IB_MAX_BID
            row[col++] = this.bid.maxBid;
            // IB_CREATED
            row[col++] = this.bid.createDate;
            // IB_UPDATED
            row[col++] = this.bid.updateDate;

            if (remaining == 0) this.updateSubTableGenerators(itemInfo);
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
            super(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 ? 1 : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : "No bids?\n" + itemInfo;

            // IMB_I_ID
            row[col++] = itemInfo.itemId;
            // IMB_U_ID
            row[col++] = itemInfo.sellerId;
            // IMB_IB_ID
            row[col++] = bid.id;
            // IMB_IB_I_ID
            row[col++] = itemInfo.itemId;
            // IMB_IB_U_ID
            row[col++] = itemInfo.sellerId;
            // IMB_CREATED
            row[col++] = bid.createDate;
            // IMB_UPDATED
            row[col++] = bid.updateDate;

            return (col);
        }
    }

    /**********************************************************************************************
     * ITEM_PURCHASE Generator
     **********************************************************************************************/
    protected class ItemPurchaseGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemPurchaseGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 && itemInfo.purchaseDate != null ? 1 : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : itemInfo;
            
            // IP_ID
            row[col++] = this.count;
            // IP_IB_ID
            row[col++] = bid.id;
            // IP_IB_I_ID
            row[col++] = itemInfo.itemId;
            // IP_IB_U_ID
            row[col++] = itemInfo.sellerId;
            // IP_DATE
            row[col++] = itemInfo.purchaseDate;

            if (profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_BUYER_LEAVES_FEEDBACK) {
                bid.buyer_feedback = true;
            }
            if (profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_SELLER_LEAVES_FEEDBACK) {
                bid.seller_feedback = true;
            }
            
            if (remaining == 0) this.updateSubTableGenerators(bid);
            return (col);
        }
    } // END CLASS
    
    /**********************************************************************************************
     * USER_FEEDBACK Generator
     **********************************************************************************************/
    protected class UserFeedbackGenerator extends SubTableGenerator<LoaderItemInfo.Bid> {

        public UserFeedbackGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_USERACCT_FEEDBACK,
                  AuctionMarkConstants.TABLENAME_ITEM_PURCHASE);
        }

        @Override
        protected short getElementCounter(LoaderItemInfo.Bid bid) {
            return (short)((bid.buyer_feedback ? 1 : 0) + (bid.seller_feedback ? 1 : 0));
        }
        @Override
        protected int populateRow(LoaderItemInfo.Bid bid, Object[] row, short remaining) {
            int col = 0;

            boolean is_buyer = false;
            if (remaining == 1 || (bid.buyer_feedback && bid.seller_feedback == false)) {
                is_buyer = true;
            } else {
                assert(bid.seller_feedback);
                is_buyer = false;
            }
            LoaderItemInfo itemInfo = bid.getLoaderItemInfo();
            
            // UF_U_ID
            row[col++] = (is_buyer ? bid.bidderId : itemInfo.sellerId);
            // UF_I_ID
            row[col++] = itemInfo.itemId;
            // UF_I_U_ID
            row[col++] = itemInfo.sellerId;
            // UF_FROM_ID
            row[col++] = (is_buyer ? itemInfo.sellerId : bid.bidderId);
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
            super(AuctionMarkConstants.TABLENAME_USERACCT_ITEM,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 && itemInfo.purchaseDate != null ? 1 : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : itemInfo;
            
            // UI_U_ID
            row[col++] = bid.bidderId;
            // UI_I_ID
            row[col++] = itemInfo.itemId;
            // UI_I_U_ID
            row[col++] = itemInfo.sellerId;
            // UI_IP_ID
            row[col++] = null;
            // UI_IP_IB_ID
            row[col++] = null;
            // UI_IP_IB_I_ID
            row[col++] = null;
            // UI_IP_IB_U_ID
            row[col++] = null;
            // UI_CREATED
            row[col++] = itemInfo.endDate;
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * USER_WATCH Generator
     **********************************************************************************************/
    protected class UserWatchGenerator extends SubTableGenerator<LoaderItemInfo> {

        final Set<UserId> watchers = new HashSet<UserId>();
        
        public UserWatchGenerator() throws SQLException {
            super(AuctionMarkConstants.TABLENAME_USERACCT_WATCH,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (itemInfo.numWatches);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            
            // Make it more likely that a user that has bid on an item is watching it
            Histogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
            UserId buyerId = null;
            int num_watchers = this.watchers.size();
            boolean use_random = (num_watchers == bidderHistogram.getValueCount());
            long num_users = tableSizes.get(AuctionMarkConstants.TABLENAME_USERACCT);
            
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("Selecting USER_WATCH buyerId [useRandom=%s, watchers=%d]",
                                        use_random, this.watchers.size()));
            int tries = 1000;
            while (buyerId == null && num_watchers < num_users && tries-- > 0) {
                try {
                    if (use_random) {
                        buyerId = profile.getRandomBuyerId();        
                    } else {
                        buyerId = profile.getRandomBuyerId(bidderHistogram, itemInfo.sellerId);
                    }
                } catch (NullPointerException ex) {
                    LOG.error("Busted Bidder Histogram:\n" + bidderHistogram);
                    throw ex;
                }
                if (this.watchers.contains(buyerId) == false) break;
                buyerId = null;
                
                // If for some reason we unable to find a buyer from our bidderHistogram,
                // then just give up and get a random one
                if (use_random == false && tries == 0) {
                    use_random = true;
                    tries = 500;
                }
            } // WHILE
            assert(buyerId != null) :
                String.format("Failed to buyer for new USER_WATCH record\n" +
                              "Tries:%d / UseRandom:%s / Watchers:%d / Users:%d / BidderHistogram:%d",
                              tries, use_random, num_watchers, num_users, bidderHistogram.getValueCount());
            this.watchers.add(buyerId);
            
            // UW_U_ID
            row[col++] = buyerId;
            // UW_I_ID
            row[col++] = itemInfo.itemId;
            // UW_I_U_ID
            row[col++] = itemInfo.sellerId;
            // UW_CREATED
            row[col++] = this.getRandomDate(itemInfo.startDate, itemInfo.endDate);

            return (col);
        }
        @Override
        protected void finishElementCallback(LoaderItemInfo t) {
            if (LOG.isTraceEnabled())
                LOG.trace("Clearing watcher cache [size=" + this.watchers.size() + "]");
            this.watchers.clear();
        }
        private Timestamp getRandomDate(Timestamp startDate, Timestamp endDate) {
            int start = Math.round(startDate.getTime() / AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
            int end = Math.round(endDate.getTime() / AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
            long offset = profile.rng.number(start, end);
            return new Timestamp(offset * AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
        }
    } // END CLASS
} // END CLASS