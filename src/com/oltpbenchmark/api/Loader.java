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
import java.util.Map;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.Table;

public abstract class Loader {

    protected final Connection conn;
    protected final WorkloadConfiguration workConf;
    protected final Map<String, Table> tables;
    
    public Loader(Connection c, WorkloadConfiguration workConf, Map<String, Table> tables) {
    	this.conn = c;
    	this.workConf = workConf;
    	this.tables = tables;
    }
	
    /**
     * 
     */
    public abstract void load();
    
}
