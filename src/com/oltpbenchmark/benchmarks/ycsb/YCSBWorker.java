package com.oltpbenchmark.benchmarks.ycsb;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;


import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.DeleteRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadModifyWriteRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ScanRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.UpdateRecord;
import com.oltpbenchmark.distributions.CounterGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;

public class YCSBWorker extends Worker{
	
	private ZipfianGenerator keysequence;
	private static CounterGenerator newRec;
	private Random rand;

	public YCSBWorker(int id, BenchmarkModule benchmarkModule) {
		super(id, benchmarkModule);
		keysequence= new ZipfianGenerator(YCSBConstants.RECORD_COUNT);
		rand=new Random(YCSBConstants.MAX_SCAN);
		newRec= new CounterGenerator(YCSBConstants.RECORD_COUNT);
	}

	@Override
	protected TransactionType doWork(boolean measure, Phase phase) {
        TransactionType nextTrans = transactionTypes.getType(phase.chooseTransaction());
        this.executeWork(nextTrans);
        return nextTrans;
	}

	@Override
	protected void executeWork(TransactionType nextTrans) {
        try {
            if (nextTrans.getProcedureClass().equals(DeleteRecord.class)) {
                deleteRecord();
            } else if (nextTrans.getProcedureClass().equals(InsertRecord.class)) {
                insertRecord();
            } else if (nextTrans.getProcedureClass().equals(ReadModifyWriteRecord.class)) {
                readModifyWriteRecord();
            } else if (nextTrans.getProcedureClass().equals(ReadRecord.class)) {
                readRecord();
            } else if (nextTrans.getProcedureClass().equals(ScanRecord.class)) {
                scanRecord();
            } else if (nextTrans.getProcedureClass().equals(UpdateRecord.class)) {
                updateRecord();
            } 
            conn.commit();

        } catch (MySQLTransactionRollbackException m) {
            System.err.println("Rollback of thread: "+ this.id+" "+ m.getMessage());
        } catch (SQLException e) {
            System.err.println("Timeout of thread: "+ this.id+" "+ e.getMessage());
        }
        return;
	}

	private void updateRecord() throws SQLException {
        UpdateRecord proc = this.getProcedure(UpdateRecord.class);
        assert (proc != null);
        int keyname = keysequence.nextInt();
        HashMap<Integer, String> values = buildValues(10);
        proc.run(conn, keyname, values);
	}

	private void scanRecord() throws SQLException {
        ScanRecord proc = this.getProcedure(ScanRecord.class);
        assert (proc != null);
        int keyname = keysequence.nextInt();
        int count=rand.nextInt();
        proc.run(conn, keyname, count, new Vector<HashMap<Integer,String>>());
	}

	private void readRecord() throws SQLException {
        ReadRecord proc = this.getProcedure(ReadRecord.class);
        assert (proc != null);
        int keyname = keysequence.nextInt();
        proc.run(conn, keyname, new HashMap<Integer,String>());
	}

	private void readModifyWriteRecord() throws SQLException {
		ReadModifyWriteRecord proc = this.getProcedure(ReadModifyWriteRecord.class);
        assert (proc != null);
        int keyname = keysequence.nextInt();
        //System.out.println("[Thread " + this.id+"] RMW this:  "+ keyname);
        proc.run(conn, keyname, new HashMap<Integer,String>());	
	}

	private void insertRecord() throws SQLException {
		InsertRecord proc = this.getProcedure(InsertRecord.class);
        assert (proc != null);
        int keyname = newRec.nextInt();
        //System.out.println("[Thread " + this.id+"] insert this:  "+ keyname);
        HashMap<Integer, String> values = buildValues(10);
        proc.run(conn, keyname, values);
	}

	private void deleteRecord() throws SQLException {
		DeleteRecord proc = this.getProcedure(DeleteRecord.class);
        assert (proc != null);
        int keyname = keysequence.nextInt();
        proc.run(conn, keyname);
	}
	
	private HashMap<Integer, String> buildValues(int numVals) {
		HashMap<Integer, String> fields=new HashMap<Integer,String>();
		for(int i=1;i<=numVals;i++)
		{
			fields.put(i, LoaderUtil.randomStr(100));
		}
		return fields;
	}

}
