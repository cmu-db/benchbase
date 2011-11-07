package com.oltpbenchmark.benchmarks.tatp;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tatp.procedures.*;

public class TATPWorker extends Worker {
	
    /**
     * Each Transaction element provides an ArgGenerator to create the proper
     * arguments used to invoke the stored procedure
     */
    private static interface ArgGenerator<T extends Procedure> {
        /**
         * Generate the proper arguments used to invoke the given stored procedure
         * @param subscriberSize
         * @return
         */
        public void genArgs(Connection conn, T proc, long subscriberSize) throws SQLException;
    }
    
    /**
     * Set of transactions structs with their appropriate parameters
     */
    public static enum Transaction {
    	DeleteCallForwarding(new ArgGenerator<DeleteCallForwarding>() {
            public void genArgs(Connection conn, DeleteCallForwarding proc, long subscriberSize) throws SQLException {
            	long s_id = TATPUtil.getSubscriberId(subscriberSize);
            	proc.run(conn,
            			 TATPUtil.padWithZero(s_id), // s_id
                         TATPUtil.number(1, 4).byteValue(), // sf_type
                         (byte)(8 * TATPUtil.number(0, 2)) // start_time
                );
            }
        }),
//        GET_ACCESS_DATA(new ArgGenerator() {
//            public Object[] genArgs(long subscriberSize) {
//                long s_id = TATPUtil.getSubscriberId(subscriberSize);
//                return new Object[] {
//                        s_id, // s_id
//                        TATPUtil.number(1, 4) // ai_type
//                };
//            }
//        }),
//        GET_NEW_DESTINATION(new ArgGenerator() {
//            public Object[] genArgs(long subscriberSize) {
//                long s_id = TATPUtil.getSubscriberId(subscriberSize);
//                return new Object[] {
//                        s_id, // s_id
//                        TATPUtil.number(1, 4), // sf_type
//                        8 * TATPUtil.number(0, 2), // start_time
//                        TATPUtil.number(1, 24) // end_time
//                };
//            }
//        }),
//        GET_SUBSCRIBER_DATA(new ArgGenerator() {
//            public Object[] genArgs(long subscriberSize) {
//                long s_id = TATPUtil.getSubscriberId(subscriberSize);
//                return new Object[] {
//                        s_id // s_id
//                };
//            }
//        }),
//        INSERT_CALL_FORWARDING(new ArgGenerator() {
//            public Object[] genArgs(long subscriberSize) {
//                long s_id = TATPUtil.getSubscriberId(subscriberSize);
//                return new Object[] {
//                        TATPUtil.padWithZero(s_id), // sub_nbr
//                        TATPUtil.number(1, 4), // sf_type
//                        8 * TATPUtil.number(0, 2), // start_time
//                        TATPUtil.number(1, 24), // end_time
//                        TATPUtil.padWithZero(s_id) // numberx
//                };
//            }
//        }),
//        UPDATE_LOCATION(new ArgGenerator() {
//            public Object[] genArgs(long subscriberSize) {
//                long s_id = TATPUtil.getSubscriberId(subscriberSize);
//                return new Object[] {
//                        TATPUtil.number(0, Integer.MAX_VALUE), // vlr_location
//                        TATPUtil.padWithZero(s_id) // sub_nbr
//                };
//            }
//        }),
//        UPDATE_SUBSCRIBER_DATA(new ArgGenerator() {
//            public Object[] genArgs(long subscriberSize) {
//                long s_id = TATPUtil.getSubscriberId(subscriberSize);
//                return new Object[] {
//                        s_id, // s_id
//                        TATPUtil.number(0, 1), // bit_1
//                        TATPUtil.number(0, 255), // data_a
//                        TATPUtil.number(1, 4) // sf_type
//                };
//            }
//        }),
        ; // END LIST OF STORED PROCEDURES
        
        /**
         * Constructor
         */
        private Transaction(ArgGenerator<? extends Procedure> ag) {
            this.ag = ag;
        }
        
        public final ArgGenerator<? extends Procedure> ag;
        
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<String, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name().toUpperCase(), vt);
            }
        }
        
        public static Transaction get(String name) {
            Transaction ret = Transaction.name_lookup.get(name.toUpperCase());
            return (ret);
        }
        
    } // TRANSCTION ENUM
    
    private final Map<TransactionType, Procedure> procedures;
    private final long subscriberSize = 10000; // FIXME
	
	public TATPWorker(Connection conn, WorkLoadConfiguration workConf, Map<TransactionType, Procedure> procedures) {
		super(conn, workConf);
		this.procedures = procedures;
	}
	
	@Override
	protected TransactionType doWork(boolean measure, Phase phase) {
		TransactionType retTP = null;
		TransactionType next = transTypes.getType(phase.chooseTransaction());
		Transaction t = Transaction.get(next.getName());
		assert(t != null) : "Unexpected " + next;
		
		// Get the Procedure handle
		Procedure proc = this.procedures.get(next);
		assert(proc != null);
		
//		t.ag.genArgs(this.conn, proc, subscriberSize);
		
		return (retTP);
	}

}
