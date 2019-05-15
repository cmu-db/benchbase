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

package com.oltpbenchmark.benchmarks.jpab;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Loader.LoaderThread;
import com.oltpbenchmark.benchmarks.jpab.tests.BasicTest;
import com.oltpbenchmark.benchmarks.jpab.tests.Test;
import com.oltpbenchmark.benchmarks.twitter.TwitterLoader;
import org.apache.log4j.Logger;

public class JPABLoader extends Loader<JPABBenchmark> {
    private static final Logger LOG = Logger.getLogger(JPABLoader.class);


    String persistanceUnit;
    public JPABLoader(JPABBenchmark benchmark, String persistanceUnit) throws SQLException {
        super(benchmark);
        this.persistanceUnit=persistanceUnit;
    }
    
    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) {
                int objectCount = (int)workConf.getScaleFactor();
                EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistanceUnit);
                EntityManager em = emf.createEntityManager();
                Test test=new BasicTest();
                test.setBatchSize(10);
                test.setEntityCount(objectCount);
                test.buildInventory(objectCount);
                while (test.getActionCount() < objectCount) {
                    LOG.debug(test.getActionCount()+ " % "+objectCount);
                    test.persist(em);
                }
                test.clearInventory();
            }
        });

        return (threads);
    }

}
