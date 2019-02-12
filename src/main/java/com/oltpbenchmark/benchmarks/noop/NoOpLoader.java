/******************************************************************************
 *  Copyright 2016 by OLTPBenchmark Project                                   *
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

package com.oltpbenchmark.benchmarks.noop;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * This doesn't load any data!
 *
 * @author pavlo
 * @author eric-haibin-lin
 */
public class NoOpLoader extends Loader<NoOpBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(NoOpLoader.class);

    public NoOpLoader(NoOpBenchmark benchmark) {
        super(benchmark);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

}
