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
import com.oltpbenchmark.benchmarks.featurebench.util.*;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RowRandomBoundedInt;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;




/**
 *
 */
public class FeatureBenchWorker extends Worker<FeatureBenchBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureBenchWorker.class);


    public FeatureBenchWorker(FeatureBenchBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }
    int get_transaction_id(int no,ArrayList<Integer> weights)
    {
        int len=weights.size();
        for(int i=0;i<len;i++)
        {
            if(no<=weights.get(i))
                return i;
        }
        return 0;
    }
    public void bind_params_based_on_func(ArrayList<BindParams> bp,PreparedStatement stmt) throws SQLException {
        for (BindParams ob : bp) {
            ArrayList<UtilityFunc> uf = ob.getUtilFunc();
            for (int j = 0; j < uf.size(); j++) {
                if (Objects.equals(uf.get(j).getName(), "RowRandomBoundedInt")) {
                    ArrayList<ParamsForUtilFunc> pfuf = uf.get(j).getParams();
                    int lower_range = pfuf.get(0).getParameters().get(0);
                    int upper_range = pfuf.get(1).getParameters().get(1);
                    RowRandomBoundedInt rno = new RowRandomBoundedInt(1, lower_range, upper_range);
                    stmt.setInt(j + 1, rno.nextValue());
                }

            }

        }
        stmt.executeQuery();

    }
    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) throws
        UserAbortException, SQLException {
        SAXBuilder saxBuilder = new SAXBuilder();

        File xmlFile = new File("src/main/resources/benchmarks/featurebench/customconfig.xml");
        try {
            Document document = saxBuilder.build(xmlFile);
            Element rootElement = document.getRootElement();
            Element parameter = (Element) rootElement.getChildren("parameters");
            String YBImplemenationClass = parameter.getChildText("microbenchmarkClass");
            YBImplemenationClass = YBImplemenationClass.substring(YBImplemenationClass.lastIndexOf('.') + 1);
            Class<?> clazz = Class.forName(YBImplemenationClass);
            Object ybm = clazz.getDeclaredConstructor().newInstance();
            ArrayList<ExecuteRule> listOfAllExecuteRules = ybm.executeRule();

            // Validating sum of transaction weights =100
            int sum=0;
            int weight;
            ArrayList<Integer> call_acc_to_weight=new ArrayList<>();
            for (ExecuteRule listOfAllExecuteRule : listOfAllExecuteRules) {
                TransactionDetails transaction_det = listOfAllExecuteRule.getTransactionDetails();
                weight = transaction_det.getWeight_transaction_type();
                sum += weight;
                call_acc_to_weight.add(sum);
            }
            if(sum>100 || sum<=0)
            {
                throw new RuntimeException("Transaction weights incorrect");
            }
            for(int i=0;i<100;i++)
            {

                int randomNum = ThreadLocalRandom.current().nextInt(1, 100 + 1);
                int getid=get_transaction_id(randomNum,call_acc_to_weight);
                TransactionDetails transaction_det=listOfAllExecuteRules.get(getid).getTransactionDetails();
                //String name_transaction = transaction_det.getName();
                //int wt=transaction_det.getWeight_transaction_type();
                ArrayList<QueryDetails> qd=transaction_det.getQuery();
                for(int j=0;j<qd.size();j++)
                {
                    String query=qd.get(i).getQuery();
                    PreparedStatement stmt=conn.prepareStatement(query);
                    ArrayList<BindParams> bp=qd.get(i).getBindParams();
                    bind_params_based_on_func(bp,stmt);
                }

            }

            return TransactionStatus.SUCCESS;

        } catch (IOException | JDOMException | ClassNotFoundException | InvocationTargetException |
                 InstantiationException |
                 IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }


    }
}

