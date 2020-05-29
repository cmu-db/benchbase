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

package com.oltpbenchmark.benchmarks.ycsb;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class YCSBBenchmark extends BenchmarkModule {

    private static final Logger LOG = LoggerFactory.getLogger(YCSBBenchmark.class);

    public YCSBBenchmark(WorkloadConfiguration workConf) {
        super(workConf, true);
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        try {


            // LOADING FROM THE DATABASE IMPORTANT INFORMATION
            // LIST OF USERS

            Table t = this.catalog.getTable("USERTABLE");

            String userCount = SQLUtil.getMaxColSQL(this.workConf.getDBType(), t, "ycsb_key");

            try (Connection metaConn = this.getConnection();
                 Statement stmt = metaConn.createStatement();
                 ResultSet res = stmt.executeQuery(userCount)) {
                int init_record_count = 0;
                while (res.next()) {
                    init_record_count = res.getInt(1);
                }

                //
                for (int i = 0; i < workConf.getTerminals(); ++i) {
                    workers.add(new YCSBWorker(this, i, init_record_count + 1));
                } // FOR
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
        return workers;
    }

    @Override
    protected Loader<YCSBBenchmark> makeLoaderImpl() throws SQLException {
        return new YCSBLoader(this);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        // TODO Auto-generated method stub
        return InsertRecord.class.getPackage();
    }

}
