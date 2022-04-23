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

package com.oltpbenchmark.benchmarks.voter;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class VoterLoader extends Loader<VoterBenchmark> {

    public VoterLoader(VoterBenchmark benchmark) {
        super(benchmark);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();

        // CONTESTANTS
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                loadContestants(conn);
            }
        });

        // LOCATIONS
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                loadLocations(conn);
            }
        });

        return threads;
    }

    private void loadContestants(Connection conn) throws SQLException {
        Table catalog_tbl = benchmark.getCatalog().getTable(VoterConstants.TABLENAME_CONTESTANTS);
        try (PreparedStatement ps = conn.prepareStatement(SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType()))) {

            for (int i = 0; i < this.benchmark.numContestants; i++) {
                ps.setInt(1, i + 1);
                ps.setString(2, VoterConstants.CONTESTANT_NAMES[i]);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void loadLocations(Connection conn) throws SQLException {
        Table catalog_tbl = benchmark.getCatalog().getTable(VoterConstants.TABLENAME_LOCATIONS);
        try (PreparedStatement ps = conn.prepareStatement(SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType()))) {

            for (int i = 0; i < VoterConstants.AREA_CODES.length; i++) {
                ps.setShort(1, VoterConstants.AREA_CODES[i]);
                ps.setString(2, VoterConstants.STATE_CODES[i]);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }
}
