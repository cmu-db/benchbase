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
import java.util.Objects;


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
    private final List<Index> indexes = new ArrayList<>();

    public Table(String name, String separator) {
        super(name, separator);
    }

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


    public void addIndex(Index index) {
        this.indexes.add(index);
    }

    public Index getIndex(String indexName) {
        for (Index catalog_idx : this.indexes) {
            if (catalog_idx.getName().equalsIgnoreCase(indexName)) {
                return (catalog_idx);
            }
        }
        return (null);
    }

    public List<Index> getIndexes() {
        return this.indexes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        Table table = (Table) o;
        return Objects.equals(columns, table.columns) &&
                Objects.equals(indexes, table.indexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columns, indexes);
    }
}
