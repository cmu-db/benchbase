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

import com.oltpbenchmark.types.SortDirectionType;

import java.io.Serializable;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class Index extends AbstractCatalogObject {
    private static final long serialVersionUID = 1L;

    private final Table table;
    private final SortedMap<Integer, IndexColumn> columns = new TreeMap<>();
    private final int type;
    private final boolean unique;

    static class IndexColumn implements Serializable {
        private final String name;
        private final SortDirectionType dir;

        IndexColumn(String name, SortDirectionType dir) {
            this.name = name;
            this.dir = dir;
        }

        public String getName() {
            return name;
        }

        public SortDirectionType getDir() {
            return dir;
        }
    }

    public Index(String name, String separator, Table table, int type, boolean unique) {
        super(name, separator);
        this.table = table;
        this.type = type;
        this.unique = unique;
    }

    public void addColumn(String colName, SortDirectionType colOrder, int colPosition) {
        this.columns.put(colPosition, new IndexColumn(colName, colOrder));
    }

    public Table getTable() {
        return table;
    }

    public SortedMap<Integer, IndexColumn> getColumns() {
        return columns;
    }

    public int getType() {
        return type;
    }

    public boolean isUnique() {
        return unique;
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
        Index index = (Index) o;
        return type == index.type &&
                unique == index.unique &&
                Objects.equals(table, index.table) &&
                Objects.equals(columns, index.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), table.name, columns, type, unique);
    }
}
