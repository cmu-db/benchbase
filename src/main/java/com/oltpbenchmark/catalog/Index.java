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
import com.oltpbenchmark.util.StringUtil;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Index extends AbstractCatalogObject {
    private static final long serialVersionUID = 1L;

    private final Table catalog_tbl;
    private final SortedMap<Integer, IndexColumn> columns = new TreeMap<>();
    private final int type;
    private final boolean unique;

    private static class IndexColumn {
        final String name;
        final SortDirectionType dir;

        IndexColumn(String name, SortDirectionType dir) {
            this.name = name;
            this.dir = dir;
        }

        @Override
        public String toString() {
            return this.name + " / " + this.dir;
        }
    }

    public Index(Table catalog_tbl, String name, int type, boolean unique) {
        super(name);
        this.catalog_tbl = catalog_tbl;
        this.type = type;
        this.unique = unique;
    }

    public Table getTable() {
        return (this.catalog_tbl);
    }

    public void addColumn(String colName, SortDirectionType colOrder, int colPosition) {

        this.columns.put(colPosition, new IndexColumn(colName, colOrder));
    }


    public int getType() {
        return this.type;
    }

    public boolean isUnique() {
        return this.unique;
    }


    public String debug() {
        Map<String, Object> m = new ListOrderedMap<>();
        m.put("Name", this.name);
        m.put("Type", this.type);
        m.put("Is Unique", this.unique);

        Map<String, Object> inner = new ListOrderedMap<>();
        for (int i = 0, cnt = this.columns.size(); i < cnt; i++) {
            IndexColumn idx_col = this.columns.get(i);
            inner.put(String.format("[%02d]", i), idx_col);
        }
        m.put("Columns", inner);

        return (StringUtil.formatMaps(m));
    }

}
