/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.catalog;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class represent a relational table.
 * 
 * @author Carlo A. Curino (carlo@curino.us)
 * @author pavlo
 */
public class Table implements Serializable {
    static final long serialVersionUID = 0;
    
    private final String tableName;
    private final List<Column> columns = new ArrayList<Column>();
    private final List<IntegrityConstraint> constraints = new ArrayList<IntegrityConstraint>();

    public Table(String tableName) {
        this.tableName = tableName;
    }
    
    public Table(Table srcTable) {
        this(srcTable.getName());

        for (int i = 0, cnt = srcTable.columns.size(); i < cnt; i++) {
            Column col = (Column)srcTable.columns.get(i).clone();
            this.columns.add(col);
        } // FOR
        for (IntegrityConstraint ic : srcTable.constraints) {
            this.constraints.add(ic.clone());
        } // FOR
    }

    @Override
    public Table clone() {
        return new Table(this);
    }

    public void addColumn(Column col) {
        assert(this.columns.contains(col) == false) : "Duplicate column name '" + col + "'";
        this.columns.add(col);
    }

    /**
     * @return the tableName
     */
    public String getName() {
        return tableName;
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

    public String getColumnName(int index) {
        return this.columns.get(index).getName();
    }

    public int indexOf(String columnName) {
        for (int i = 0, cnt = getColumnCount(); i < cnt; i++) {
            if (this.columns.get(i).getName().equalsIgnoreCase(columnName)) {
                return (i);
            }
        } // FOR
        return -1;
    }

    public Column getColumnByName(String colname) {
        int idx = indexOf(colname);
        return (idx >= 0 ? this.columns.get(idx) : null);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        sb.append(getName()).append(" (\n");
        for (int i = 0, cnt = this.columns.size(); i < cnt; i++) {
            sb.append("  ").append(this.columns.get(i)).append("\n");
        } // FOR
        sb.append(")");

        return (sb.toString());
    }

    public void addConstraint(List<IntegrityConstraint> iclist) throws IntegrityConstraintsExistsException {
        for (IntegrityConstraint ic : iclist) {
            addConstraint(ic);
        }
    }

    public void addConstraint(IntegrityConstraint ic) throws IntegrityConstraintsExistsException {
        for (IntegrityConstraint c : constraints) {
            if (c != null && c.getId().equals(ic.getId()))
                throw new IntegrityConstraintsExistsException("A constraint " + ic.getId() + " already exists in this table");
        }
        constraints.add(ic);
    }

    @Override
    public boolean equals(Object object) {
        if ((object instanceof Table) == false) return (false);

        Table table2 = (Table)object;
        return (this.tableName.equals(table2.tableName) &&
                this.columns.equals(table2.columns) &&
                this.constraints.equals(table2.constraints));
    }

}
