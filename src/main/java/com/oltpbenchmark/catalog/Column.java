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

/**
 * Column Catalog Object
 * @author pavlo
 */
public class Column extends AbstractCatalogObject implements Cloneable {
	private static final long serialVersionUID = 1L;
	
	private final Table catalog_tbl;
    private final int type;
    private final String typename;
    private final Integer size;

    private String defaultValue;
    private boolean nullable = false;
    private boolean autoincrement = false;
    private boolean signed = false;
    private Column foreignkey = null;
    
    public Column(Table catalog_tbl, String name, int type, String typename, Integer size) {
    	super(name);
        this.catalog_tbl = catalog_tbl;
        this.type = type;
        this.typename = typename;
        this.size = size;
    }
    
    @Override
    protected Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Table getTable() {
        return (this.catalog_tbl);
    }
    
    public int getIndex() {
        return (this.catalog_tbl.getColumnIndex(this));
    }
    
    public String fullName() {
        return String.format("%s.%s", this.catalog_tbl.getName(), this.name);
    }
    
    /**
     * @return the type
     */
    public int getType() {
        return type;
    }

    /**
     * @return the typename
     */
    public String getTypename() {
        return typename;
    }

    /**
     * Return the max size of this column
     * @return the size
     */
    public Integer getSize() {
        return size;
    }
    
    /**
     * @return the defaultValue
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * @return the nullable
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * @return the autoincrement
     */
    public boolean isAutoincrement() {
        return autoincrement;
    }

    /**
     * @return the signed
     */
    public boolean isSigned() {
        return signed;
    }
    
    /**
     * @return the foreign key parent for this column
     */
    public Column getForeignKey() {
        return foreignkey;
    }
    
    /**
     * @param defaultValue the defaultValue to set
     */
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }
    
    /**
     * @param nullable the nullable to set
     */
    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    /**
     * @param autoincrement the autoincrement to set
     */
    public void setAutoincrement(boolean autoincrement) {
        this.autoincrement = autoincrement;
    }

    /**
     * @param signed the signed to set
     */
    public void setSigned(boolean signed) {
        this.signed = signed;
    }

    /**
     * @param foreignkey the foreign key parent for this column
     */
    public void setForeignKey(Column foreignkey) {
        this.foreignkey = foreignkey;
    }
    
    @Override
    public String toString() {
        return (this.getName());
    }
    
}
