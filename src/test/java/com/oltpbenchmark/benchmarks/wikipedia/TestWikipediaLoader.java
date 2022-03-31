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

package com.oltpbenchmark.benchmarks.wikipedia;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.api.Procedure;

import java.util.List;

public class TestWikipediaLoader extends AbstractTestLoader<WikipediaBenchmark> {

    private static final String[] IGNORE = {
            WikipediaConstants.TABLENAME_IPBLOCKS,
            WikipediaConstants.TABLENAME_LOGGING,
            WikipediaConstants.TABLENAME_PAGE_BACKUP,
            WikipediaConstants.TABLENAME_PAGE_RESTRICTIONS,
            WikipediaConstants.TABLENAME_RECENTCHANGES,
            WikipediaConstants.TABLENAME_REVISION,
            WikipediaConstants.TABLENAME_TEXT,
            WikipediaConstants.TABLENAME_USER_GROUPS,
            WikipediaConstants.TABLENAME_VALUE_BACKUP,
    };

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestWikipediaBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<WikipediaBenchmark> benchmarkClass() {
        return WikipediaBenchmark.class;
    }

    @Override
    public List<String> ignorableTables() {
        return List.of(IGNORE);
    }
}
