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

import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.voter.PhoneCallGenerator.PhoneCall;
import com.oltpbenchmark.benchmarks.voter.procedures.Vote;
import com.oltpbenchmark.types.TransactionStatus;

public class VoterWorker extends Worker<VoterBenchmark> {

    private final PhoneCallGenerator switchboard;
    
    public VoterWorker(VoterBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        switchboard = new PhoneCallGenerator(0, benchmarkModule.numContestants);
    }

    @Override
    protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {
        assert (txnType.getProcedureClass().equals(Vote.class));
        PhoneCall call = switchboard.receive();
        Vote proc = getProcedure(Vote.class);
        assert (proc != null);
        proc.run(conn, call.voteId, call.phoneNumber, call.contestantNumber, VoterConstants.MAX_VOTES);
        conn.commit();
        return TransactionStatus.SUCCESS;
    }

}
