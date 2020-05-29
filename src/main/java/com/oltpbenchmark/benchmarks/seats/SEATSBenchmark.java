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

package com.oltpbenchmark.benchmarks.seats;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.seats.procedures.LoadConfig;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SEATSBenchmark extends BenchmarkModule {

    private static final Logger LOG = LoggerFactory.getLogger(SEATSBenchmark.class);


    private final RandomGenerator rng = new RandomGenerator((int) System.currentTimeMillis());

    public SEATSBenchmark(WorkloadConfiguration workConf) {
        super(workConf, true);
        this.registerSupplementalProcedure(LoadConfig.class);
    }

    public File getDataDir() {
        URL url = SEATSBenchmark.class.getClassLoader().getResource("benchmarks" + File.separator + getBenchmarkName());
        try {
            if (url != null) {
                return new File(url.toURI().getPath());
            }
        } catch (URISyntaxException e) {
            LOG.error(e.getMessage(), e);
        }
        return (null);
    }

    public RandomGenerator getRandomGenerator() {
        return (this.rng);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return (LoadConfig.class.getPackage());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        for (int i = 0; i < this.workConf.getTerminals(); ++i) {
            workers.add(new SEATSWorker(this, i));
        } // FOR
        return (workers);
    }

    @Override
    protected Loader<SEATSBenchmark> makeLoaderImpl() throws SQLException {
        return new SEATSLoader(this);
    }

    /**
     * Return the path of the CSV file that has data for the given Table catalog
     * handle
     *
     * @param data_dir
     * @param catalog_tbl
     * @return
     */
    public static final File getTableDataFile(File data_dir, Table catalog_tbl) {
        File f = new File(String.format("%s%stable.%s.csv", data_dir.getAbsolutePath(), File.separator, catalog_tbl.getName().toLowerCase()));

        return (f);
    }
}
