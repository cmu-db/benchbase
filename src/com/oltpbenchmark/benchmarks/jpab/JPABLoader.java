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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.jpab.tests.BasicTest;
import com.oltpbenchmark.benchmarks.jpab.tests.Test;

public class JPABLoader extends Loader<JPABBenchmark> {

    String persistanceUnit;
    public JPABLoader(JPABBenchmark benchmark, Connection conn, String persistanceUnit) throws SQLException {
        super(benchmark, conn);
        this.persistanceUnit=persistanceUnit;
    }

    @Override
    public void load() throws SQLException {
        int objectCount= (int)this.workConf.getScaleFactor();
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistanceUnit);
        EntityManager em = emf.createEntityManager();
        Test test=new BasicTest();
        test.setBatchSize(10);
        test.setEntityCount(objectCount);
        test.buildInventory(objectCount); 
        while (test.getActionCount() < objectCount) {
            System.out.println(test.getActionCount()+ " % "+objectCount);
            test.persist(em);
        }
        test.clearInventory();
    }

}
