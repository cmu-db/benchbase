package com.oltpbenchmark.benchmarks.voter;

import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.voter.PhoneCallGenerator.PhoneCall;
import com.oltpbenchmark.benchmarks.voter.procedures.Vote;
import com.oltpbenchmark.types.TransactionStatus;

public class VoterWorker extends Worker {

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
