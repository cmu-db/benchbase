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
package com.oltpbenchmark.benchmarks.replay.procedures;

import com.oltpbenchmark.api.SQLStmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import com.oltpbenchmark.api.Procedure;

/**
 * @brief A Procedure which can represent an arbitrary list of SQLStmts which
 *        is passed in as an argument at runtime.
 * 
 * The reason it takes in a list of SQLStmts is so that it can represent
 * multi-statement transactions.
 */
public class DynamicProcedure extends Procedure {
    List<SQLStmt> sqlStmts;

    DynamicProcedure(List<SQLStmt> sqlStmts) {
        System.out.println("DynamicProcedure() called");
        this.sqlStmts = sqlStmts;
    }

    public void run(Connection conn) throws SQLException {
        System.out.println("DynamicProcedure.run() called");
        for (SQLStmt sqlStmt : this.sqlStmts) {
            PreparedStatement preparedStatement = this.getPreparedStatement(conn, sqlStmt);
            preparedStatement.execute();
        }
    }
}
