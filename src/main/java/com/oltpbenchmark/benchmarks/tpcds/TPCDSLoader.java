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

package com.oltpbenchmark.benchmarks.tpcds;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TPCDSLoader extends Loader<TPCDSBenchmark> {
    public TPCDSLoader(TPCDSBenchmark benchmark) {
        super(benchmark);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();
        final CountDownLatch custAddrLatch = new CountDownLatch(1);
        final CountDownLatch custDemLatch = new CountDownLatch(1);
        final CountDownLatch dateLatch = new CountDownLatch(1);
        final CountDownLatch incomeLatch = new CountDownLatch(1);
        final CountDownLatch itemLatch = new CountDownLatch(1);
        final CountDownLatch reasonLatch = new CountDownLatch(1);
        final CountDownLatch shipModeLatch = new CountDownLatch(1);
        final CountDownLatch timeLatch = new CountDownLatch(1);
        final CountDownLatch warehouseLatch = new CountDownLatch(1);
        final CountDownLatch promoLatch = new CountDownLatch(1);
        final CountDownLatch householdLatch = new CountDownLatch(1);
        final CountDownLatch storeLatch = new CountDownLatch(1);
        final CountDownLatch customerLatch = new CountDownLatch(1);
        final CountDownLatch webPageLatch = new CountDownLatch(1);
        final CountDownLatch webSiteLatch = new CountDownLatch(1);
        final CountDownLatch callCenterLatch = new CountDownLatch(1);
        final CountDownLatch catalogPageLatch = new CountDownLatch(1);
        final CountDownLatch storeSalesLatch = new CountDownLatch(1);
        final CountDownLatch catalogSalesLatch = new CountDownLatch(1);
        final CountDownLatch webSalesLatch = new CountDownLatch(1);


        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_CALLCENTER, TPCDSConstants.callcenterTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    callCenterLatch.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_CATALOGPAGE, TPCDSConstants.catalogpageTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    catalogPageLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_STORE, TPCDSConstants.storeTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    storeLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_WEBSITE, TPCDSConstants.websiteTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    webSiteLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_HOUSEHOLDDEM, TPCDSConstants.householddemTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    incomeLatch.await();
                    householdLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_PROMOTION, TPCDSConstants.promotionTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    itemLatch.await();
                    promoLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_INVENTORY, TPCDSConstants.inventoryTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    itemLatch.await();
                    warehouseLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_CUSTOMER, TPCDSConstants.customerTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    householdLatch.await();
                    customerLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {

                loadTable(conn, TPCDSConstants.TABLENAME_WEBPAGE, TPCDSConstants.webpageTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    customerLatch.await();
                    webPageLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_STORESALES, TPCDSConstants.storesalesTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    promoLatch.await();
                    timeLatch.await();
                    storeLatch.await();
                    storeSalesLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_STORERETURNS, TPCDSConstants.storereturnsTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    reasonLatch.await();
                    timeLatch.await();
                    storeLatch.await();
                    storeSalesLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_WEBSALES, TPCDSConstants.websalesTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    promoLatch.await();
                    timeLatch.await();
                    webPageLatch.await();
                    shipModeLatch.await();
                    warehouseLatch.await();
                    webSiteLatch.await();
                    webSalesLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {


                loadTable(conn, TPCDSConstants.TABLENAME_WEBRETURNS, TPCDSConstants.webreturnsTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    reasonLatch.await();
                    timeLatch.await();
                    webPageLatch.await();
                    webSalesLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTable(conn, TPCDSConstants.TABLENAME_CATALOGSALES, TPCDSConstants.catalogsalesTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    callCenterLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    promoLatch.await();
                    timeLatch.await();
                    shipModeLatch.await();
                    warehouseLatch.await();
                    catalogPageLatch.await();
                    catalogSalesLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

            }
        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
                loadTable(conn, TPCDSConstants.TABLENAME_CATALOGRETURNS, TPCDSConstants.catalogreturnsTypes);
            }

            @Override
            public void beforeLoad() {
                try {
                    dateLatch.await();
                    custAddrLatch.await();
                    custDemLatch.await();
                    customerLatch.await();
                    callCenterLatch.await();
                    householdLatch.await();
                    itemLatch.await();
                    reasonLatch.await();
                    timeLatch.await();
                    shipModeLatch.await();
                    warehouseLatch.await();
                    catalogPageLatch.await();
                    catalogSalesLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        return threads;
    }


    private void loadTable(Connection conn, String tableName, TPCDSConstants.CastTypes[] types) throws SQLException {

    }
}
