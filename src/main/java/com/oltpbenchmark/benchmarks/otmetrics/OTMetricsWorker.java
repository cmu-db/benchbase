/*
 * Copyright 2022 by OLTPBenchmark Project
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

package com.oltpbenchmark.benchmarks.otmetrics;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.otmetrics.procedures.GetSessionRange;
import com.oltpbenchmark.types.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * OtterTune Metrics Timeseries Benchmark
 * @author pavlo
 */
public class OTMetricsWorker extends Worker<OTMetricsBenchmark> {

    private static final Logger LOG = LoggerFactory.getLogger(OTMetricsWorker.class);

    public OTMetricsWorker(OTMetricsBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans) throws UserAbortException, SQLException {
        if (nextTrans.getProcedureClass().equals(GetSessionRange.class)) {
            execGetSessionRange(conn);
        }
        return (TransactionStatus.SUCCESS);
    }

    public void execGetSessionRange(Connection conn) throws SQLException {
        int session_low = rng().nextInt(this.getBenchmark().num_sessions);
        int session_high =  session_low + rng().nextInt(5) * this.getBenchmark().num_sources;
        int source_id = session_low % this.getBenchmark().num_sources;

        int type_category = (int)Math.floor(source_id / OTMetricsConstants.NUM_TYPES);
        int type_id = (rng().nextInt(OTMetricsConstants.NUM_TYPES) % OTMetricsConstants.NUM_TYPES);
        int num_types = rng().nextInt(3) + 1;
        int types[] = new int[num_types];
        for (int i = 0; i < types.length; i++) {
            types[i] = type_id + type_category + i;
        };

        GetSessionRange proc = this.getProcedure(GetSessionRange.class);
        List<Object[]> result = proc.run(conn, source_id, session_low, session_high, types);

        if (LOG.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("source_id=").append(source_id).append(" ");
            sb.append("session_low=").append(session_low).append(" ");
            sb.append("session_high=").append(session_high).append(" ");
            sb.append("type_id=").append(Arrays.toString(types)).append(" ");
            sb.append(" -> ").append(result.size()).append(" results");
            LOG.debug(sb.toString());
        }
    }


}
