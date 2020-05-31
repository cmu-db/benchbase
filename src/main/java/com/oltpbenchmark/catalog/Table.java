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


package com.oltpbenchmark.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Table Catalog Object
 *
 * @author Carlo A. Curino (carlo@curino.us)
 * @author pavlo
 * @author Djellel
 */
public class Table extends AbstractCatalogObject {
    private static final long serialVersionUID = 1L;

    private final List<Column> columns = new ArrayList<>();
    private final List<IntegrityConstraint> constraints = new ArrayList<>();
    private final List<Index> indexes = new ArrayList<>();


    public Table(String tableName) {
        super(tableName);
    }

    public Table(Table srcTable) {
        this(srcTable.getName());

        for (int i = 0, cnt = srcTable.columns.size(); i < cnt; i++) {
            Column col = (Column) srcTable.columns.get(i).clone();
            this.columns.add(col);
        }
        for (IntegrityConstraint ic : srcTable.constraints) {
            this.constraints.add(ic.clone());
        }
    }

    @Override
    public Table clone() {
        return new Table(this);
    }

    // ----------------------------------------------------------
    // COLUMNS
    // ----------------------------------------------------------

    public void addColumn(Column col) {

        this.columns.add(col);
    }

    public int getColumnCount() {
        return this.columns.size();
    }

    public List<Column> getColumns() {
        return Collections.unmodifiableList(this.columns);
    }

    public Column getColumn(int index) {
        return this.columns.get(index);
    }

    public int getColumnIndex(Column catalog_col) {
        return (this.getColumnIndex(catalog_col.getName()));
    }

    public int getColumnIndex(String columnName) {
        for (int i = 0, cnt = getColumnCount(); i < cnt; i++) {
            if (this.columns.get(i).getName().equalsIgnoreCase(columnName)) {
                return (i);
            }
        }
        return -1;
    }

    public int[] getColumnTypes() {
        int[] types = new int[this.getColumnCount()];
        for (Column catalog_col : this.getColumns()) {
            types[catalog_col.getIndex()] = catalog_col.getType();
        }
        return (types);
    }

    public Column getColumnByName(String colname) {
        int idx = getColumnIndex(colname);
        return (idx >= 0 ? this.columns.get(idx) : null);
    }

    // ----------------------------------------------------------
    // INDEXES
    // ----------------------------------------------------------

    /**
     * Add a new Index for this table
     *
     * @param index
     */
    public void addIndex(Index index) {

        this.indexes.add(index);
    }

    /**
     * Return a particular index based on its name
     *
     * @param indexName
     * @return
     */
    public Index getIndex(String indexName) {
        for (Index catalog_idx : this.indexes) {
            if (catalog_idx.getName().equalsIgnoreCase(indexName)) {
                return (catalog_idx);
            }
        }
        return (null);
    }

    // ----------------------------------------------------------
    // PRIMARY KEY INDEX
    // ----------------------------------------------------------



    @Override
    public boolean equals(Object object) {
        if (!(object instanceof Table)) {
            return (false);
        }

        Table table2 = (Table) object;
        return (this.name.equals(table2.name) &&
                this.columns.equals(table2.columns) &&
                this.constraints.equals(table2.constraints));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(getName()).append(" (\n");
        for (Column column : this.columns) {
            sb.append("  ").append(column).append("\n");
        }
        sb.append(")");

        return (sb.toString());
    }
}
