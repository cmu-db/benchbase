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

package com.oltpbenchmark.benchmarks.linkbench.procedures;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.pojo.Link;
import com.oltpbenchmark.api.Procedure;

public class UpdateLink extends Procedure{
    
    private static final Logger LOG = Logger.getLogger(UpdateLink.class);

    public void run(Connection conn, Link l, boolean noinverse) throws SQLException {
        // executed through addLink Procedure
    }
}
