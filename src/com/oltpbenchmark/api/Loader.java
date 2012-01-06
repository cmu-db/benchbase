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
package com.oltpbenchmark.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.Table;

public abstract class Loader {

    protected final BenchmarkModule benchmark;
    protected final Connection conn;
    protected final WorkloadConfiguration workConf;
    protected final Map<String, Table> tables;
    protected final double scaleFactor;
    
    /**
     * TODO: We need a way to set the seed value so that it is
     * 		 uniform throughout the entire benchmark. We should probably
     * 		 stick this in the BenchmarkModule base class.
     */
    private final Random rand = new Random();
    
    public Loader(BenchmarkModule benchmark, Connection conn) {
        this.benchmark = benchmark;
    	this.conn = conn;
    	this.workConf = benchmark.getWorkloadConfiguration();
    	this.tables = benchmark.getTables(this.conn);
    	this.scaleFactor = workConf.getScaleFactor();
    }
    
    /**
     * Get the number of tables loaded from the catalog
     * @return
     */
    public int getTableCount() {
        return (this.tables.size());
    }
	
    /**
     * Get the catalog object for the given table name
     * @param tableName
     * @return
     */
    public Table getTableCatalog(String tableName) {
        Table catalog_tbl = this.tables.get(tableName.toUpperCase());
        assert(catalog_tbl != null) : "Invalid table name '" + tableName + "' " + this.tables.keySet();
        return (catalog_tbl);
    }

    /**
     * Get the pre-seeded Random generator for this Loader invocation
     * @return
     */
    public Random rng() {
    	return (this.rand);
    }
    
    /**
     * @throws SQLException 
     * 
     */
    public abstract void load() throws SQLException;
    
}
