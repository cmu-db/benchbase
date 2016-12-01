package com.oltpbenchmark.benchmarks.smallbank;

import java.sql.SQLException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.smallbank.procedures.Amalgamate;
import com.oltpbenchmark.benchmarks.smallbank.procedures.Balance;
import com.oltpbenchmark.benchmarks.smallbank.procedures.DepositChecking;
import com.oltpbenchmark.benchmarks.smallbank.procedures.SendPayment;
import com.oltpbenchmark.benchmarks.smallbank.procedures.TransactSavings;
import com.oltpbenchmark.benchmarks.smallbank.procedures.WriteCheck;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RandomDistribution.*;

/**
 * SmallBank Benchmark Work Driver
 * Fuck yo couch.
 * @author pavlo
 * 
 */
public class SmallBankWorker extends Worker<SmallBankBenchmark> {
    private static final Logger LOG = Logger.getLogger(SmallBankWorker.class);
    
    private final Amalgamate procAmalgamate;
    private final Balance procBalance;
    private final DepositChecking procDepositChecking;
    private final SendPayment procSendPayment;
    private final TransactSavings procTransactSavings;
    private final WriteCheck procWriteCheck;
    
    private final DiscreteRNG rng;
    private final long numAccounts;
    private final int custNameLength;
    private final String custNameFormat;
    private final long custIdsBuffer[] = { -1l, -1l };
    
    public SmallBankWorker(SmallBankBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        
        // This is a minor speed-up to avoid having to invoke the hashmap look-up
        // everytime we want to execute a txn. This is important to do on 
        // a client machine with not a lot of cores
        this.procAmalgamate = this.getProcedure(Amalgamate.class);
        this.procBalance = this.getProcedure(Balance.class);
        this.procDepositChecking = this.getProcedure(DepositChecking.class);
        this.procSendPayment = this.getProcedure(SendPayment.class);
        this.procTransactSavings = this.getProcedure(TransactSavings.class);
        this.procWriteCheck = this.getProcedure(WriteCheck.class);
        
        this.numAccounts = benchmarkModule.numAccounts;
        this.custNameLength = SmallBankBenchmark.getCustomerNameLength(benchmarkModule.getTableCatalog(SmallBankConstants.TABLENAME_ACCOUNTS));
        this.custNameFormat = "%0"+this.custNameLength+"d";
        this.rng = new Flat(rng(), 0, this.numAccounts);
    }
    
    protected void generateCustIds(boolean needsTwoAccts) {
        for (int i = 0; i < this.custIdsBuffer.length; i++) {
            this.custIdsBuffer[i] = this.rng.nextLong();
            
            // They can never be the same!
            if (i > 0 && this.custIdsBuffer[i-1] == this.custIdsBuffer[i]) {
                i--;
                continue;
            }
            
            // If we only need one acctId, break out here.
            if (i == 0 && needsTwoAccts == false) break;
            // If we need two acctIds, then we need to go generate the second one
            if (i == 0) continue;
            
        } // FOR
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Accounts: %s", Arrays.toString(this.custIdsBuffer)));
    }
    

    @Override
    protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = txnType.getProcedureClass();
        
        // Amalgamate
        if (procClass.equals(Amalgamate.class)) {
            this.generateCustIds(true);
            this.procAmalgamate.run(conn, this.custIdsBuffer[0], this.custIdsBuffer[1]);
            
        // Balance
        } else if (procClass.equals(Balance.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            this.procBalance.run(conn, custName);
            
        // DepositChecking
        } else if (procClass.equals(DepositChecking.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            this.procDepositChecking.run(conn, custName, SmallBankConstants.PARAM_DEPOSIT_CHECKING_AMOUNT);
            
        // SendPayment
        } else if (procClass.equals(SendPayment.class)) {
            this.generateCustIds(true);
            this.procSendPayment.run(conn, this.custIdsBuffer[0], this.custIdsBuffer[0], SmallBankConstants.PARAM_SEND_PAYMENT_AMOUNT);
            
        // TransactSavings
        } else if (procClass.equals(TransactSavings.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            this.procTransactSavings.run(conn, custName, SmallBankConstants.PARAM_TRANSACT_SAVINGS_AMOUNT);
            
        // WriteCheck
        } else if (procClass.equals(WriteCheck.class)) {
            this.generateCustIds(false);
            String custName = String.format(this.custNameFormat, this.custIdsBuffer[0]);
            this.procWriteCheck.run(conn, custName, SmallBankConstants.PARAM_WRITE_CHECK_AMOUNT);
            
        }
        conn.commit();
        
        return TransactionStatus.SUCCESS;
    }

}
