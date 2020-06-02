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

/**
 * Column Catalog Object
 *
 * @author pavlo
 */
public class Column extends AbstractCatalogObject {
    private static final long serialVersionUID = 1L;

    private final Table table;
    private final int type;
    private final Integer size;
    private final boolean nullable;

    private Column foreignKey = null;

    public Column(String name, String separator, Table table, int type, Integer size, boolean nullable) {
        super(name, separator);
        this.table = table;
        this.type = type;
        this.size = size;
        this.nullable = nullable;
    }

    public Table getTable() {
        return table;
    }

    public int getType() {
        return type;
    }

    public Integer getSize() {
        return size;
    }

    public boolean isNullable() {
        return nullable;
    }

    public Column getForeignKey() {
        return foreignKey;
    }

    public void setForeignKey(Column foreignKey) {
        this.foreignKey = foreignKey;
    }

    public int getIndex() {
        return this.table.getColumnIndex(this);
    }
}
