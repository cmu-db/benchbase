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

package com.oltpbenchmark.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.collections15.map.ListOrderedMap;

import com.oltpbenchmark.types.SortDirectionType;
import com.oltpbenchmark.util.StringUtil;

public class Index extends AbstractCatalogObject {
    private static final long serialVersionUID = 1l;
    
    private final Table catalog_tbl;
    private final SortedMap<Integer, IndexColumn> columns = new TreeMap<Integer, Index.IndexColumn>();
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
    } // CLASS
    
    public Index(Table catalog_tbl, String name, int type, boolean unique) {
        super(name);
        this.catalog_tbl = catalog_tbl;
        this.type = type;
        this.unique = unique;
    }
    
    public Table getTable() {
        return (this.catalog_tbl);
    }
    
    public String fullName() {
        return String.format("%s.%s", this.catalog_tbl.getName(), this.name);
    }
    
    public void addColumn(String colName, SortDirectionType colOrder, int colPosition) {
        assert(this.columns.containsKey(colPosition) == false);
        this.columns.put(colPosition, new IndexColumn(colName, colOrder));
    }
    
    /**
     * Get the number of columns that are part of this index
     * @return
     */
    public int getColumnCount() {
        return (this.columns.size());
    }

    /**
     * Return an ordered list of all the columns that are part of this index
     * @return
     */
    public Collection<String> getColumnNames() {
        List<String> colNames = new ArrayList<String>();
        for (IndexColumn idx_col : this.columns.values()) {
            colNames.add(idx_col.name);
        } // FOR
        return (colNames);
    }
    
    public String getColumnName(int position) {
        IndexColumn idx_col = this.columns.get(position);
        return (idx_col != null ? idx_col.name : null);
    }
    
    public SortDirectionType getColumnDirection(int position) {
        IndexColumn idx_col = this.columns.get(position);
        return (idx_col != null ? idx_col.dir : null);
    }
    
    
    public int getType() {
        return this.type;
    }
    public boolean isUnique() {
        return this.unique;
    }
    

    public String debug() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Name", this.name);
        m.put("Type", this.type);
        m.put("Is Unique", this.unique);
        
        Map<String, Object> inner = new ListOrderedMap<String, Object>();
        for (int i = 0, cnt = this.columns.size(); i < cnt; i++) {
            IndexColumn idx_col = this.columns.get(i);
            inner.put(String.format("[%02d]", i), idx_col);
        } // FOR
        m.put("Columns", inner);
        
        return (StringUtil.formatMaps(m));
    }
    
}
