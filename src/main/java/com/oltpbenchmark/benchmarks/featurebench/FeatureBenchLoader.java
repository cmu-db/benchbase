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

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.featurebench.CustomBenchmark.YBMicroBenchmarkImplementation;
import com.oltpbenchmark.benchmarks.featurebench.util.*;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;




public class FeatureBenchLoader extends Loader<FeatureBenchBenchmark> {

    PreparedStatement stmt;

    public FeatureBenchLoader(FeatureBenchBenchmark benchmark) {

        super(benchmark);

    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        SAXBuilder saxBuilder = new SAXBuilder();

        File xmlFile = new File("src/main/resources/benchmarks/featurebench/customconfig.xml");
        try {
            Document document = saxBuilder.build(xmlFile);
            Element rootElement = document.getRootElement();
            Element parameter = (Element) rootElement.getChildren("parameters");
            String YBImplemenationClass = parameter.getChildText("microbenchmarkClass");
            YBImplemenationClass = YBImplemenationClass.substring(YBImplemenationClass.lastIndexOf('.') + 1);
            Class<?> clazz = Class.forName(YBImplemenationClass);
            YBMicroBenchmarkImplementation ybm = (YBMicroBenchmarkImplementation) clazz.getDeclaredConstructor().newInstance();

            ArrayList<LoadRule> listOfAllLoadRules = ybm.loadRule();
            ArrayList<LoaderThread> lt = new ArrayList<>();

            for (LoadRule listOfAllLoadRule : listOfAllLoadRules) {
                lt.add(new Generator(listOfAllLoadRule));
            }

            return lt;

        } catch (IOException | JDOMException io) {
            System.out.println(io.getMessage());
        } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private class Generator extends LoaderThread {
        final LoadRule lr;



        public Generator(LoadRule loadRule) {
            super(benchmark);
            lr = loadRule;
        }

        public void bindParamBasedOnType(UtilityFunc uf) throws SQLException {


           if(Objects.equals(uf.getName(), "get_int_primary_key"))
           {
               ArrayList<ParamsForUtilFunc> ob1=uf.getParams();
               ParamsForUtilFunc puf=ob1.get(0);
               ArrayList<Integer> range=puf.getParameters();
               int upper_range=range.get(1);
               int lower_range=range.get(0);
               UtilGenerators.setUpper_range_for_primary_int_keys(upper_range);
               UtilGenerators.setLower_range_for_primary_int_keys(lower_range);
           }
           else if(Objects.equals(uf.getName(), "numberToIdString"))
           {
               ArrayList<ParamsForUtilFunc> ob1=uf.getParams();
               ParamsForUtilFunc puf=ob1.get(0);
               ArrayList<Integer> max_len=puf.getParameters();
               int desired_len=max_len.get(0);
               UtilGenerators.setDesired_length_string_pkeys(desired_len);
           }

        }

        public String findFuncname(UtilityFunc uf){
            if(Objects.equals(uf.getName(), "get_int_primary_key"))
            {
                return "get_int_primary_key";
            }
            else if(Objects.equals(uf.getName(), "numberToIdString")){
                return "numberToIdString";
            }
            else return null;

        }

        @Override
        public void load(Connection conn) throws SQLException {


            try {
                int batchSize = 0;
                TableInfo t = lr.getTableInfo();
                long no_of_rows = t.getNo_of_rows();
                String table_name = t.get_table_name();
                ArrayList<columnsDetails> cd = t.getColumn_Det();
                int no_of_columns = cd.size();
                StringBuilder columnString = new StringBuilder();
                StringBuilder valueString = new StringBuilder();


                for (int i = 0; i < no_of_columns; i++) {
                    columnString.append(cd.get(i).getName()).append(",");
                    valueString.append("?,");
                }
                columnString.setLength(columnString.length() - 1);
                valueString.setLength(valueString.length() - 1);
                String insertStmt = "INSERT INTO" + table_name + "(" + columnString + ")VALUES" + "(" + valueString + ")";
                stmt = conn.prepareStatement(insertStmt);


                  for(int i=0;i<no_of_rows;i++) {
                      for (int j = 0; j < no_of_columns; j++) {
                          UtilityFunc uf = cd.get(j).getUtilFunc();
                          bindParamBasedOnType(uf);
                      }
                  }

                  for(int i=0;i<no_of_rows;i++)
                  {
                      for(int j=0;j<no_of_columns;j++) {
                          UtilityFunc uf = cd.get(j).getUtilFunc();
                          String funcname = findFuncname(uf);
                          if (Objects.equals(funcname, "get_int_primary_key")) {
                              stmt.setInt(j + 1, UtilGenerators.get_int_primary_key());
                          } else if (Objects.equals(funcname, "numberToIdString")) {
                              stmt.setString(j + 1, UtilGenerators.numberToIdString());
                          }
                      }
                      stmt.addBatch();
                      if (++batchSize >= workConf.getBatchSize()) {
                          this.loadTables(conn);
                          batchSize = 0;
                      }
                  }
                stmt.executeBatch();
                if (batchSize > 0) {
                    this.loadTables(conn);
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

        private void loadTables(Connection conn) throws SQLException {
            stmt.executeBatch();
        }
    }

}









