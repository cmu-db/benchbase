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

package com.oltpbenchmark.benchmarks.voter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Loader.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import org.apache.log4j.Logger;
import org.hibernate.exception.SQLGrammarException;

public class VoterLoader extends Loader<VoterBenchmark> {

    private static final Logger LOG = Logger.getLogger(VoterLoader.class);

    // Domain data: matching lists of Area codes and States
    private static final short[] areaCodes = new short[]{
        907,205,256,334,251,870,501,479,480,602,623,928,520,341,764,628,831,925,
        909,562,661,510,650,949,760,415,951,209,669,408,559,626,442,530,916,627,
        714,707,310,323,213,424,747,818,858,935,619,805,369,720,303,970,719,860,
        203,959,475,202,302,689,407,239,850,727,321,754,954,927,352,863,386,904,
        561,772,786,305,941,813,478,770,470,404,762,706,678,912,229,808,515,319,
        563,641,712,208,217,872,312,773,464,708,224,847,779,815,618,309,331,630,
        317,765,574,260,219,812,913,785,316,620,606,859,502,270,504,985,225,318,
        337,774,508,339,781,857,617,978,351,413,443,410,301,240,207,517,810,278,
        679,313,586,947,248,734,269,989,906,616,231,612,320,651,763,952,218,507,
        636,660,975,816,573,314,557,417,769,601,662,228,406,336,252,984,919,980,
        910,828,704,701,402,308,603,908,848,732,551,201,862,973,609,856,575,957,
        505,775,702,315,518,646,347,212,718,516,917,845,631,716,585,607,914,216,
        330,234,567,419,440,380,740,614,283,513,937,918,580,405,503,541,971,814,
        717,570,878,835,484,610,267,215,724,412,401,843,864,803,605,423,865,931,
        615,901,731,254,325,713,940,817,430,903,806,737,512,361,210,979,936,409,
        972,469,214,682,832,281,830,956,432,915,435,801,385,434,804,757,703,571,
        276,236,540,802,509,360,564,206,425,253,715,920,262,414,608,304,307};

    private static final String[] states = new String[] {
        "AK","AL","AL","AL","AL","AR","AR","AR","AZ","AZ","AZ","AZ","AZ","CA","CA",
        "CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA",
        "CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA",
        "CA","CA","CA","CA","CO","CO","CO","CO","CT","CT","CT","CT","DC","DE","FL",
        "FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL",
        "FL","FL","FL","GA","GA","GA","GA","GA","GA","GA","GA","GA","HI","IA","IA",
        "IA","IA","IA","ID","IL","IL","IL","IL","IL","IL","IL","IL","IL","IL","IL",
        "IL","IL","IL","IN","IN","IN","IN","IN","IN","KS","KS","KS","KS","KY","KY",
        "KY","KY","LA","LA","LA","LA","LA","MA","MA","MA","MA","MA","MA","MA","MA",
        "MA","MD","MD","MD","MD","ME","MI","MI","MI","MI","MI","MI","MI","MI","MI",
        "MI","MI","MI","MI","MI","MN","MN","MN","MN","MN","MN","MN","MO","MO","MO",
        "MO","MO","MO","MO","MO","MS","MS","MS","MS","MT","NC","NC","NC","NC","NC",
        "NC","NC","NC","ND","NE","NE","NH","NJ","NJ","NJ","NJ","NJ","NJ","NJ","NJ",
        "NJ","NM","NM","NM","NV","NV","NY","NY","NY","NY","NY","NY","NY","NY","NY",
        "NY","NY","NY","NY","NY","OH","OH","OH","OH","OH","OH","OH","OH","OH","OH",
        "OH","OH","OK","OK","OK","OR","OR","OR","PA","PA","PA","PA","PA","PA","PA",
        "PA","PA","PA","PA","RI","SC","SC","SC","SD","TN","TN","TN","TN","TN","TN",
        "TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX",
        "TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","UT","UT","UT","VA","VA",
        "VA","VA","VA","VA","VA","VA","VT","WA","WA","WA","WA","WA","WA","WI","WI",
        "WI","WI","WI","WV","WY"};

    public VoterLoader(VoterBenchmark benchmark) {
        super(benchmark);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();

        // CONTESTANTS
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                VoterLoader.this.loadContestants(conn);
            }
        });

        // LOCATIONS
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                VoterLoader.this.loadLocations(conn);
            }
        });

        return threads;
    }

    private void loadContestants(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(VoterConstants.TABLENAME_CONTESTANTS);
        PreparedStatement ps = conn.prepareStatement(SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType()));
        String[] contestants = VoterConstants.CONTESTANT_NAMES_CSV.split(",");

        for (int i = 0; i < this.benchmark.numContestants; i++) {
            ps.setInt(1, i+1);
            ps.setString(2, contestants[i]);
            ps.addBatch();
        }
        ps.executeBatch();
        conn.commit();
        ps.close();
    }

    private void loadLocations(Connection conn) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(VoterConstants.TABLENAME_LOCATIONS);
        PreparedStatement ps = conn.prepareStatement(SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType()));

        for (int i = 0; i < areaCodes.length; i++) {
            ps.setShort(1, areaCodes[i]);
            ps.setString(2, states[i]);
            ps.addBatch();
        }
        ps.executeBatch();
        conn.commit();
        ps.close();
    }
}
