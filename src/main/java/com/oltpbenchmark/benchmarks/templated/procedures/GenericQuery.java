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
                String paramType = paramsTypes[i].toLowerCase();
                switch (paramType) {
                    case "integer":
                        int min = param.getMin().intValue();
                        int max = param.getMax().intValue();

                        switch (distribution) {

                            case "uniform":
                                Random uniformRand = (Random) param.getGen();
                                stmt.setInt(i + 1, uniformRand.nextInt(min, max));
                                break;
                            case "binomial":
                                Random binomialRandom = (Random) param.getGen();
                                int bVal;
                                do {
                                    bVal = (int) (min + Math.abs(binomialRandom.nextGaussian()) * max);
                                } while (bVal > max || bVal < min);

                                stmt.setInt(i + 1, bVal);
                                break;
                            case "zipf":
                                ZipfianGenerator zipfGen = (ZipfianGenerator) param.getGen();
                                stmt.setInt(i + 1, zipfGen.nextInt());
                                break;
                            case "scrambled":
                                ScrambledZipfianGenerator scramGen = (ScrambledZipfianGenerator) param.getGen();
                                stmt.setInt(i + 1, scramGen.nextInt());
                                break;
                            default:
                                throw new RuntimeException(
                                        "Distribution: " + distribution + " not supported for type: " + paramType);
                        }
                        break;
                    case "float":
                    case "real":
                        float minF = Float.parseFloat(param.getMinString());
                        float maxF = Float.parseFloat(param.getMaxString());
                        switch (distribution) {

                            case "uniform":
                                Random uniformRand = (Random) param.getGen();
                                stmt.setFloat(i + 1, uniformRand.nextFloat(minF, maxF));
                                break;
                            case "binomial":
                                Random binomialRandom = (Random) param.getGen();
                                float fVal;
                                do {
                                    fVal = (float) (minF + Math.abs(binomialRandom.nextGaussian()) * maxF);
                                } while (fVal > maxF || fVal < minF);

                                stmt.setFloat(i + 1, fVal);
                                break;
                            default:
                                throw new RuntimeException(
                                        "Distribution: " + distribution + " not supported for type: " + paramType);
                        }
                        break;
                    case "bigint":
                        Long minL = param.getMin();
                        Long maxL = param.getMax();
                        switch (distribution) {

                            case "uniform":
                                Random uniformRand = (Random) param.getGen();
                                stmt.setLong(i + 1, uniformRand.nextLong(minL, maxL));
                                break;
                            case "binomial":
                                Random binomialRandom = (Random) param.getGen();
                                Long lVal;
                                do {
                                    lVal = Double.valueOf(minL + Math.abs(binomialRandom.nextGaussian()) * maxL)
                                            .longValue();
                                } while (lVal > maxL || lVal < minL);

                                stmt.setLong(i + 1, lVal);
                                break;
                            case "zipf":
                                ZipfianGenerator zipfGen = (ZipfianGenerator) param.getGen();
                                stmt.setLong(i + 1, zipfGen.nextLong());
                                break;
                            case "scrambled":
                                ScrambledZipfianGenerator scramGen = (ScrambledZipfianGenerator) param.getGen();
                                stmt.setLong(i + 1, scramGen.nextLong());
                                break;
                            default:
                                throw new RuntimeException(
                                        "Distribution: " + distribution + " not supported for type: " + paramType);
                        }
                        break;
                    case "varchar":
                    case "string":
                        switch (distribution) {
                            case "uniform":
                                Random strRandom = (Random) param.getGen();
                                stmt.setString(i + 1, TextGenerator.randomStr(strRandom, param.getMax().intValue()));
                                break;
                            default:
                                throw new RuntimeException(
                                        "Distribution: " + distribution + " not supported for type: " + paramType);
                        }
                        break;
                    case "timestamp":
                        Long stampMin = param.getMin();
                        Long stampMax = param.getMax();
                        switch (distribution) {
                            case "uniform":
                                Random uniformRand = (Random) param.getGen();
                                stmt.setTimestamp(i + 1, new Timestamp(uniformRand.nextLong(stampMin, stampMax)));
                                break;
                            case "binomial":
                                Random binomialRandom = (Random) param.getGen();
                                Long lVal;
                                do {
                                    lVal = Double.valueOf(stampMin + Math.abs(binomialRandom.nextGaussian()) * stampMax)
                                            .longValue();
                                } while (lVal > stampMax || lVal < stampMin);

                                stmt.setTimestamp(i + 1, new Timestamp(lVal));
                                break;
                            case "zipf":
                                ZipfianGenerator zipfGen = (ZipfianGenerator) param.getGen();
                                stmt.setTimestamp(i + 1, new Timestamp(zipfGen.nextLong()));
                                break;
                            case "scrambled":
                                ScrambledZipfianGenerator scramGen = (ScrambledZipfianGenerator) param.getGen();
                                stmt.setTimestamp(i + 1, new Timestamp(scramGen.nextLong()));
                                break;
                            default:
                                throw new RuntimeException(
                                        "Distribution: " + distribution + " not supported for type: " + paramType);
                        }
                        break;
                    case "date":
                        Long dateMin = param.getMin();
                        Long dateMax = param.getMax();
                        switch (distribution) {
                            case "uniform":
                                Random uniformRand = (Random) param.getGen();
                                stmt.setDate(i + 1, new Date(uniformRand.nextLong(dateMin, dateMax)));
                                break;
                            case "binomial":
                                Random binomialRandom = (Random) param.getGen();
                                Long lVal;
                                do {
                                    lVal = Double.valueOf(dateMin + Math.abs(binomialRandom.nextGaussian()) * dateMax)
                                            .longValue();
                                } while (lVal > dateMax || lVal < dateMin);

                                stmt.setDate(i + 1, new Date(lVal));
                                break;
                            case "zipf":
                                ZipfianGenerator zipfGen = (ZipfianGenerator) param.getGen();
                                stmt.setDate(i + 1, new Date(zipfGen.nextLong()));
                                break;
                            case "scrambled":
                                ScrambledZipfianGenerator scramGen = (ScrambledZipfianGenerator) param.getGen();
                                stmt.setDate(i + 1, new Date(scramGen.nextLong()));
                                break;
                            default:
                                throw new RuntimeException(
                                        "Distribution: " + distribution + " not supported for type: " + paramType);
                        }
                        break;
                    case "time":
                        Long timeMin = param.getMin();
                        Long timeMax = param.getMax();
                        switch (distribution) {
                            case "uniform":
                                Random uniformRand = (Random) param.getGen();
                                stmt.setTime(i + 1, new Time(uniformRand.nextLong(timeMin, timeMax)));
                                break;
                            case "binomial":
                                Random binomialRandom = (Random) param.getGen();
                                Long lVal;
                                do {
                                    lVal = Double.valueOf(timeMin + Math.abs(binomialRandom.nextGaussian()) * timeMax)
                                            .longValue();
                                } while (lVal > timeMax || lVal < timeMin);

                                stmt.setTime(i + 1, new Time(lVal));
                                break;
                            case "zipf":
                                ZipfianGenerator zipfGen = (ZipfianGenerator) param.getGen();
                                stmt.setTime(i + 1, new Time(zipfGen.nextLong()));
                                break;
                            case "scrambled":
                                ScrambledZipfianGenerator scramGen = (ScrambledZipfianGenerator) param.getGen();
                                stmt.setTime(i + 1, new Time(scramGen.nextLong()));
                                break;
                            default:
                                throw new RuntimeException(
                                        "Distribution: " + distribution + " not supported for type: " + paramType);
                        }
                        break;
                    default:
                        throw new RuntimeException(
                                "Support for distributions for the type: " + paramType + " is current not implemented");
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
