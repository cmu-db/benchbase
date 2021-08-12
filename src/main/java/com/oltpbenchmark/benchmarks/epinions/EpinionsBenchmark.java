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


package com.oltpbenchmark.benchmarks.epinions;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetAverageRatingByTrustedUser;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.SQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class EpinionsBenchmark extends BenchmarkModule {

    private static final Logger LOG = LoggerFactory.getLogger(EpinionsBenchmark.class);

    public EpinionsBenchmark(WorkloadConfiguration workConf) {
        super(workConf);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return GetAverageRatingByTrustedUser.class.getPackage();
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        DatabaseType databaseType = this.getWorkloadConfiguration().getDatabaseType();

        try {


            // LOADING FROM THE DATABASE IMPORTANT INFORMATION
            // LIST OF USERS

            Table t = this.getCatalog().getTable("USERACCT");


            ArrayList<String> user_ids = new ArrayList<>();
            ArrayList<String> item_ids = new ArrayList<>();
            String userCount = SQLUtil.selectColValues(databaseType, t, "u_id");

            try (Connection metaConn = this.makeConnection()) {
                try (Statement stmt = metaConn.createStatement()) {
                    try (ResultSet res = stmt.executeQuery(userCount)) {
                        while (res.next()) {
                            user_ids.add(res.getString(1));
                        }
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Loaded: {} User ids", user_ids.size());
                    }
                    // LIST OF ITEMS AND
                    t = this.getCatalog().getTable("ITEM");


                    String itemCount = SQLUtil.selectColValues(databaseType, t, "i_id");
                    try (ResultSet res = stmt.executeQuery(itemCount)) {
                        while (res.next()) {
                            item_ids.add(res.getString(1));
                        }
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Loaded: {} Item ids", item_ids.size());
                }
            }

            // Now create the workers.
            for (int i = 0; i < workConf.getTerminals(); ++i) {
                workers.add(new EpinionsWorker(this, i, user_ids, item_ids));
            }

        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
        return workers;
    }

    @Override
    protected Loader<EpinionsBenchmark> makeLoaderImpl() {
        return new EpinionsLoader(this);
    }

}
