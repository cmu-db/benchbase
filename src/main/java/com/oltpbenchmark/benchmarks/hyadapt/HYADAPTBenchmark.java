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

package com.oltpbenchmark.benchmarks.hyadapt;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.hyadapt.procedures.ReadRecord1;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class HYADAPTBenchmark extends BenchmarkModule {
    private static final Logger LOG = LoggerFactory.getLogger(HYADAPTBenchmark.class);

    public HYADAPTBenchmark(WorkloadConfiguration workConf) {
        super(workConf);
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();


        // LOADING FROM THE DATABASE IMPORTANT INFORMATION
        // LIST OF USERS

        Table t = this.getCatalog().getTable("HTABLE");

        String userCount = SQLUtil.getCountSQL(this.workConf.getDatabaseType(), t);
        int init_record_count = 0;
        try (Connection metaConn = this.makeConnection()) {

            try (Statement stmt = metaConn.createStatement()) {
                try (ResultSet res = stmt.executeQuery(userCount)) {
                    while (res.next()) {
                        init_record_count = res.getInt(1);
                    }

                }
            }
            //
            for (int i = 0; i < workConf.getTerminals(); ++i) {
//                Connection conn = this.makeConnection();
//                conn.setAutoCommit(false);
                workers.add(new HYADAPTWorker(this, i, init_record_count + 1));
            }

            LOG.info("Init Record Count :: {}", init_record_count);
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }

        return workers;
    }

    @Override
    protected Loader<HYADAPTBenchmark> makeLoaderImpl() {
        return new HYADAPTLoader(this);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        // TODO Auto-generated method stub
        return ReadRecord1.class.getPackage();
    }

}
