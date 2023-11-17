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
package com.oltpbenchmark.benchmarks.templated.procedures;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Random;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.templated.util.ComplexValue;
import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.TextGenerator;

public abstract class GenericQuery extends Procedure {

    protected static final Logger LOG = LoggerFactory.getLogger(GenericQuery.class);

    /** Execution method with parameters. */
    public void run(Connection conn, List<ComplexValue> params)
            throws SQLException {
        try (PreparedStatement stmt = getStatement(conn, params);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                // do nothing
            }
        }
        conn.commit();
    }

    /** Execution method without parameters. */
    public void run(Connection conn) throws SQLException {
        QueryTemplateInfo queryTemplateInfo = this.getQueryTemplateInfo();

        try (PreparedStatement stmt = this.getPreparedStatement(conn, queryTemplateInfo.getQuery());
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                // do nothing
            }
        }
        conn.commit();
    }

    public PreparedStatement getStatement(Connection conn, List<ComplexValue> params) throws SQLException {
        QueryTemplateInfo queryTemplateInfo = this.getQueryTemplateInfo();

        PreparedStatement stmt = this.getPreparedStatement(conn, queryTemplateInfo.getQuery());
        String[] paramsTypes = queryTemplateInfo.getParamsTypes();
        for (int i = 0; i < paramsTypes.length; i++) {

            ComplexValue param = params.get(i);
            boolean hasDist = param.getDist().length() > 0;
            boolean hasValue = param.getValue().length() > 0;

            if ((!hasDist && !hasValue) || paramsTypes[i].equalsIgnoreCase("NULL")) {
                stmt.setNull(i + 1, Types.NULL);
            
            } else if (hasDist) {
                String distribution = param.getDist();
                switch (distribution) {
                    case "uniform":
                        Random rnd = (Random) param.getGen();

                        switch (paramsTypes[i].toLowerCase()) {
                            case "integer":
                                int min = param.getMin().intValue();
                                int max = param.getMax().intValue();
                                stmt.setInt(i+1, rnd.nextInt(min,max));
                                break;
                            case "float":
                            case "real":
                                float minF = Float.parseFloat(param.getMinString());
                                float maxF = Float.parseFloat(param.getMaxString());
                                stmt.setFloat(i+1, rnd.nextFloat(minF,maxF));
                                break;
                            case "bigint":
                                stmt.setLong(i+1,rnd.nextLong(param.getMin(), param.getMax()));
                            case "string": 
                                int maxLen = param.getMax().intValue();
                                stmt.setString(i + 1, TextGenerator.randomStr(rnd, maxLen));
                                break;
                            case "timestamp":
                                stmt.setTimestamp(i + 1, new Timestamp(rnd.nextLong(param.getMin(), param.getMax())));
                                break;
                            case "date":
                                stmt.setDate(i + 1, new Date(rnd.nextLong(param.getMin(), param.getMax())));
                                break;
                            case "time":
                                stmt.setTime(i + 1, new Time(rnd.nextLong(param.getMin(), param.getMax())));
                                break;
                            default:
                                throw new RuntimeException("The type '" + paramsTypes[i] + "' is not supported by the '" + distribution + "' distribution");
                        }
                        break;
                    case "binomial":
                        break;
                     /*
                        minI = Integer.parseInt(params.get(index + 1).toString());
                        maxI = Integer.parseInt(params.get(index + 2).toString());
                        Random rng = (Random) randomGenerators.get(index);
                        int bVal;
                        do {
                            bVal = (int) (minI + Math.abs(rng.nextGaussian()) * maxI);
                        } while (bVal > maxI || bVal < minI);
                        stmt.setInt(i + 1, bVal);
                        j += 2;
                        break;

                    case "zipf":
                        ZipfianGenerator zipfGen = (ZipfianGenerator) randomGenerators.get(index);
                        stmt.setInt(i + 1, zipfGen.nextInt());
                        zipfGen.n
                        j += 2;
                        break;
                    
                   
                    case "scrambled":
                        ScrambledZipfianGenerator scramZipf = (ScrambledZipfianGenerator) randomGenerators.get(index);
                        stmt.setInt(i + 1, scramZipf.nextInt());
                        j += 2;
                        break;
                    case "string":
                        maxI = Integer.parseInt(params.get(index + 1).toString());
                        Random rand = (Random) randomGenerators.get(index);
                        String randText = TextGenerator.randomStr(rand, maxI);
                        stmt.setString(i + 1, randText);
                        j += 1;
                        break;
                    case "datetime":
                    case "timestamp":
                        stmt.setTimestamp(i + 1, new Timestamp(System.currentTimeMillis()));
                        break;
                    case "date":
                        stmt.setDate(i + 1, new Date(System.currentTimeMillis()));
                        break;
                    case "time":
                        stmt.setTime(i + 1, new Time(System.currentTimeMillis()));
                        break;
                        */
                    default:
                        throw new RuntimeException("No suitable distribution found: " + distribution);
                }

            } else {
                try {
                    Object val = param.getValue();
                    stmt.setObject(i + 1, val,
                            Integer.parseInt(Types.class.getDeclaredField(paramsTypes[i]).get(null).toString()));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(
                            "Error when setting parameters. Parameter type: " + paramsTypes[i] + ", parameter value: "
                                    + param.getValue());
                }
            }
        }
        System.out.println(stmt.toString());
        return stmt;
    }

    public abstract QueryTemplateInfo getQueryTemplateInfo();

    @Value.Immutable
    public interface QueryTemplateInfo {

        /** Query string for this template. */
        SQLStmt getQuery();

        /** Query parameter types. */
        String[] getParamsTypes();

        /** Potential query parameter values. */
        ComplexValue[] getParamsValues();
    }

}
