/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.epinions;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetAverageRatingByTrustedUser;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

public class EpinionsBenchmark extends BenchmarkModule {
    
    private static final Logger LOG = Logger.getLogger(EpinionsBenchmark.class);

    public EpinionsBenchmark(WorkloadConfiguration workConf) {
        super("epinions", workConf, true);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return GetAverageRatingByTrustedUser.class.getPackage();
    }

    @Override
    protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
        ArrayList<Worker> workers = new ArrayList<Worker>();

        try {
            Connection metaConn = this.makeConnection();

            // LOADING FROM THE DATABASE IMPORTANT INFORMATION
            // LIST OF USERS

            Table t = this.catalog.getTable("USER");
            assert (t != null) : "Invalid table name '" + t + "' " + this.catalog.getTables();

            String userCount = SQLUtil.selectColValues(t, "u_id");
            Statement stmt = metaConn.createStatement();
            ResultSet res = stmt.executeQuery(userCount);
            ArrayList<String> user_ids = new ArrayList<String>();
            while (res.next()) {
                user_ids.add(res.getString(1));
            }
            res.close();
            if(LOG.isDebugEnabled()) LOG.debug("Loaded: "+user_ids.size()+" User ids");
            // LIST OF ITEMS AND
            t = this.catalog.getTable("ITEM");
            assert (t != null) : "Invalid table name '" + t + "' " + this.catalog.getTables();
            String itemCount = SQLUtil.selectColValues(t, "i_id");
            res = stmt.executeQuery(itemCount);
            ArrayList<String> item_ids = new ArrayList<String>();
            while (res.next()) {
                item_ids.add(res.getString(1));
            }
            res.close();
            if(LOG.isDebugEnabled()) LOG.debug("Loaded: "+item_ids.size()+" Item ids");
            metaConn.close();
            // Now create the workers.
            for (int i = 0; i < workConf.getTerminals(); ++i) {
                workers.add(new EpinionsWorker(i, this, user_ids, item_ids));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return workers;
    }

    @Override
    protected Loader makeLoaderImpl(Connection conn) throws SQLException {
        return new EpinionsLoader(this, conn);
    }

}
