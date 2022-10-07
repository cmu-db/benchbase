/*
 * Copyright 2020 by OLTPBenchmark Project
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

package com.oltpbenchmark.benchmarks.featurebench;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.featurebench.helpers.*;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RandomGenerator;
import com.oltpbenchmark.util.RowRandomBoundedInt;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;


/**
 *
 */
public class FeatureBenchWorker extends Worker<FeatureBenchBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureBenchWorker.class);
    static boolean isCleanUpDone = false;
    public String workloadClass = null;
    public HierarchicalConfiguration<ImmutableNode> config = null;
    public YBMicroBenchmark ybm = null;

    public FeatureBenchWorker(FeatureBenchBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }

    int get_transaction_id(int no, ArrayList<Integer> weights) {
        int len = weights.size();
        for (int i = 0; i < len; i++) {
            if (no <= weights.get(i))
                return i;
        }
        return 0;
    }

    public void bind_params_based_on_func(ArrayList<BindParams> bp, PreparedStatement stmt) throws SQLException {
        for (BindParams ob : bp) {
            ArrayList<UtilityFunc> ufs = ob.getUtilFunc();
            for (int j = 0; j < ufs.size(); j++) {
                if (Objects.equals(ufs.get(j).getName(), "RandomNoWithinRange")) {
                    ArrayList<ParamsForUtilFunc> pfuf = ufs.get(j).getParams();
                    int lower_range = pfuf.get(0).getParameters().get(0);
                    int upper_range = pfuf.get(0).getParameters().get(1);
                    RowRandomBoundedInt rno = new RowRandomBoundedInt(1, lower_range, upper_range);
                    stmt.setInt(j + 1, rno.nextValue());
                } else if (Objects.equals(ufs.get(j).getName(), "astring")) {
                    ArrayList<ParamsForUtilFunc> pfuf = ufs.get(j).getParams();
                    int min_len = pfuf.get(0).getParameters().get(0);
                    int max_len = pfuf.get(0).getParameters().get(1);
                    int randomNum = ThreadLocalRandom.current().nextInt(1, 100 + 1);
                    if (randomNum % 2 == 0) {
                        RandomGenerator rno = new RandomGenerator(1);
                        String dname = rno.astring(min_len, max_len);
                        stmt.setString(j + 1, dname);
                    }
                }
            }
        }
        stmt.executeQuery();
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) throws
        UserAbortException, SQLException {

        try {
            ybm = (YBMicroBenchmark) Class.forName(workloadClass)
                .getDeclaredConstructor(HierarchicalConfiguration.class)
                .newInstance(config);

            System.out.println(this.configuration.getWorkloadState().getGlobalState());
            if (ybm.executeOnceImplemented) {
                ybm.executeOnce(conn);
                conn.close();
            } else {
                ArrayList<ExecuteRule> executeRules = ybm.executeRules();
                // Validating sum of transaction weights =100
                int sum = 0;
                int weight;
                ArrayList<Integer> call_acc_to_weight = new ArrayList<>();
                for (ExecuteRule executeRule : executeRules) {
                    TransactionDetails transaction_det = executeRule.getTransactionDetails();
                    weight = transaction_det.getWeight_transaction_type();
                    sum += weight;
                    call_acc_to_weight.add(sum);
                }
                if (sum > 100 || sum <= 0) {
                    throw new RuntimeException("Transaction weights incorrect");
                }
                for (int i = 0; i < 100; i++) {
                    int randomNum = ThreadLocalRandom.current().nextInt(1, 100 + 1);
                    int getId = get_transaction_id(randomNum, call_acc_to_weight);
                    TransactionDetails transaction_det = executeRules.get(getId).getTransactionDetails();
                    ArrayList<QueryDetails> qd = transaction_det.getQuery();
                    for (QueryDetails queryDetails : qd) {
                        String query = queryDetails.getQuery();
                        PreparedStatement stmt = conn.prepareStatement(query);
                        ArrayList<BindParams> bp = queryDetails.getBindParams();
                        bind_params_based_on_func(bp, stmt);
                    }

                }
            }

            return TransactionStatus.SUCCESS;

        } catch (ClassNotFoundException | InvocationTargetException |
                 InstantiationException |
                 IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void tearDown() {

        if (!this.configuration.getNewConnectionPerTxn() && this.conn != null && ybm != null) {
            try {
                if (this.configuration.getWorkloadState().getGlobalState() == State.EXIT && !isCleanUpDone) {
                    if (config.containsKey("cleanup")) {
                        LOG.info("\n=================Cleanup Phase taking from Yaml=========\n");
                        List<String> ddls = config.getList(String.class, "cleanup");
                        try {
                            Statement stmtOBj = conn.createStatement();
                            for (String ddl : ddls) {
                                stmtOBj.execute(ddl);
                            }
                            stmtOBj.close();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }

                    } else {
                        ybm.cleanUp(conn);
                    }
                    isCleanUpDone = true;
                }
                conn.close();
            } catch (SQLException e) {
                LOG.error("Connection couldn't be closed.", e);
            }
        }
    }
}
