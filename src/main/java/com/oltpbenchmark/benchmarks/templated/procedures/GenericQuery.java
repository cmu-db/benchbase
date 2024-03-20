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

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.templated.util.TemplatedValue;
import com.oltpbenchmark.benchmarks.templated.util.ValueGenerator;
import com.oltpbenchmark.util.JDBCSupportedType;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class GenericQuery extends Procedure {

  protected static final Logger LOG = LoggerFactory.getLogger(GenericQuery.class);

  /** Execution method with parameters. */
  public void run(Connection conn, List<TemplatedValue> params) throws SQLException {

    try (PreparedStatement stmt = getStatement(conn, params)) {
      boolean hasResultSet = stmt.execute();
      if (hasResultSet) {
        do {
          try (ResultSet rs = stmt.getResultSet()) {
            while (rs.next()) {
              // do nothing
            }
          } catch (Exception resultException) {
            resultException.printStackTrace();
            throw new RuntimeException("Could not retrieve ResultSet");
          }
        } while (stmt.getMoreResults());
      } else {
        // Case for UPDATE, INSERT, DELETE queries
        // do nothing
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(
          String.format(
              "Error when trying to execute statement with params:\n%s\n%s",
              this.getQueryTemplateInfo(), params));
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

  public PreparedStatement getStatement(Connection conn, List<TemplatedValue> params)
      throws SQLException {
    QueryTemplateInfo queryTemplateInfo = this.getQueryTemplateInfo();

    PreparedStatement stmt = this.getPreparedStatement(conn, queryTemplateInfo.getQuery());
    String[] paramsTypes = queryTemplateInfo.getParamsTypes();
    for (int i = 0; i < paramsTypes.length; i++) {

      TemplatedValue param = params.get(i);
      boolean hasDist = param.getDistribution() != null;
      boolean hasValue = param.getValue() != null;

      JDBCSupportedType paramType = JDBCSupportedType.valueOf(paramsTypes[i].toUpperCase());

      if ((!hasDist && !hasValue) || paramType == JDBCSupportedType.NULL) {
        stmt.setNull(i + 1, Types.NULL);
      } else if (hasDist) {
        ValueGenerator distribution = param.getDistribution();
        switch (paramType) {
          case INTEGER:
            int generatedInt;
            switch (distribution) {
              case UNIFORM:
                generatedInt = param.getNextLongUniform().intValue();
                break;
              case NORMAL:
                generatedInt = param.getNextLongBinomial().intValue();
                break;
              case ZIPFIAN:
                generatedInt = param.getNextLongZipf().intValue();
                break;
              case SCRAMBLED:
                generatedInt = param.getNextLongScrambled().intValue();
                break;
              default:
                throw param.createRuntimeException();
            }
            stmt.setInt(i + 1, generatedInt);
            break;
          case FLOAT:
          case REAL:
            float generatedFloat;
            switch (distribution) {
              case UNIFORM:
                generatedFloat = param.getNextFloatUniform();
                break;
              case NORMAL:
                generatedFloat = param.getNextFloatBinomial();
                break;
              default:
                throw param.createRuntimeException();
            }
            stmt.setFloat(i + 1, generatedFloat);
            break;
          case BIGINT:
            Long generatedLong;
            switch (distribution) {
              case UNIFORM:
                generatedLong = param.getNextLongUniform();
                break;
              case NORMAL:
                generatedLong = param.getNextLongBinomial();
                break;
              case ZIPFIAN:
                generatedLong = param.getNextLongZipf();
                break;
              case SCRAMBLED:
                generatedLong = param.getNextLongScrambled();
                break;
              default:
                throw param.createRuntimeException();
            }
            stmt.setLong(i + 1, generatedLong);
            break;
          case VARCHAR:
            switch (distribution) {
              case UNIFORM:
                stmt.setString(i + 1, param.getNextString());
                break;
              default:
                throw param.createRuntimeException();
            }
            break;
          case TIMESTAMP:
          case DATE:
          case TIME:
            Long generatedTimestamp;
            switch (distribution) {
              case UNIFORM:
                generatedTimestamp = param.getNextLongUniform();
                break;
              case NORMAL:
                generatedTimestamp = param.getNextLongBinomial();
                break;
              case ZIPFIAN:
                generatedTimestamp = param.getNextLongZipf();
                break;
              case SCRAMBLED:
                generatedTimestamp = param.getNextLongScrambled();
                break;
              default:
                throw param.createRuntimeException();
            }
            if (paramType == JDBCSupportedType.TIMESTAMP) {
              stmt.setTimestamp(i + 1, new Timestamp(generatedTimestamp));
            } else if (paramType == JDBCSupportedType.DATE) {
              stmt.setDate(i + 1, new Date(generatedTimestamp));
            } else {
              stmt.setTime(i + 1, new Time(generatedTimestamp));
            }
            break;
          default:
            throw new RuntimeException(
                "Support for distributions for the type: "
                    + paramType
                    + " is not currently implemented");
        }

      } else {
        try {
          Object val = param.getValue();
          stmt.setObject(
              i + 1,
              val,
              Integer.parseInt(Types.class.getDeclaredField(paramsTypes[i]).get(null).toString()));
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(
              "Error when setting parameters. Parameter type: "
                  + paramsTypes[i]
                  + ", parameter value: "
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
    TemplatedValue[] getParamsValues();
  }
}
