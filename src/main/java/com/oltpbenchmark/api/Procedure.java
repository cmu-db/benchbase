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

package com.oltpbenchmark.api;

import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(Procedure.class);

    private final String procName;
    private DatabaseType dbType;
    private Map<String, SQLStmt> name_stmt_xref;

    /**
     * Constructor
     */
    protected Procedure() {
        this.procName = this.getClass().getSimpleName();
    }

    /**
     * Initialize all of the SQLStmt handles. This must be called separately from
     * the constructor, otherwise we can't get access to all of our SQLStmts.
     *
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    protected final <T extends Procedure> T initialize(DatabaseType dbType) {
        this.dbType = dbType;
        this.name_stmt_xref = Procedure.getStatements(this);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Initialized %s with %d SQLStmts: %s",
                    this, this.name_stmt_xref.size(), this.name_stmt_xref.keySet()));
        }
        return ((T) this);
    }

    /**
     * Return the name of this Procedure
     */
    protected final String getProcedureName() {
        return (this.procName);
    }

    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt.
     * This will automatically call setObject for all the parameters you pass in
     *
     * @param conn
     * @param stmt
     * @param params
     * @return
     * @throws SQLException
     */
    public final PreparedStatement getPreparedStatement(Connection conn, SQLStmt stmt, Object... params) throws SQLException {
        PreparedStatement pStmt = this.getPreparedStatementReturnKeys(conn, stmt, null);
        for (int i = 0; i < params.length; i++) {
            pStmt.setObject(i + 1, params[i]);
        }
        return (pStmt);
    }

    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt.
     *
     * @param conn
     * @param stmt
     * @param is
     * @return
     * @throws SQLException
     */
    public final PreparedStatement getPreparedStatementReturnKeys(Connection conn, SQLStmt stmt, int[] is) throws SQLException {

        PreparedStatement pStmt = null;

        // HACK: If the target system is Postgres, wrap the PreparedStatement in a special
        //       one that fakes the getGeneratedKeys().
        if (is != null && (
                this.dbType == DatabaseType.POSTGRES
                || this.dbType == DatabaseType.COCKROACHDB
                || this.dbType == DatabaseType.SQLSERVER
                || this.dbType == DatabaseType.SQLAZURE
            )
        ) {
            pStmt = new AutoIncrementPreparedStatement(this.dbType, conn.prepareStatement(stmt.getSQL()));
        }
        // Everyone else can use the regular getGeneratedKeys() method
        else if (is != null) {
            pStmt = conn.prepareStatement(stmt.getSQL(), is);
        }
        // They don't care about keys
        else {
            pStmt = conn.prepareStatement(stmt.getSQL());
        }

        return (pStmt);
    }

    /**
     * Fetch the SQL from the dialect map
     *
     * @param dialects
     */
    protected final void loadSQLDialect(StatementDialects dialects) {
        Collection<String> stmtNames = dialects.getStatementNames(this.procName);
        if (stmtNames == null) {
            return;
        }
        for (String stmtName : stmtNames) {
            String sql = dialects.getSQL(this.procName, stmtName);


            SQLStmt stmt = this.name_stmt_xref.get(stmtName);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Setting %s SQL dialect for %s.%s",
                        dialects.getDatabaseType(), this.procName, stmtName));
            }
            if (stmt == null) {
                throw new RuntimeException(String.format("Dialect file contains an unknown statement: Procedure %s, Statement %s", this.procName, stmtName));
            }
            stmt.setSQL(sql);
        }
    }

    /**
     * Hook for testing
     *
     * @return
     */
    protected final Map<String, SQLStmt> getStatements() {
        return (Collections.unmodifiableMap(this.name_stmt_xref));
    }

    protected static Map<String, SQLStmt> getStatements(Procedure proc) {
        Class<? extends Procedure> c = proc.getClass();
        Map<String, SQLStmt> stmts = new HashMap<>();
        for (Field f : c.getDeclaredFields()) {
            int modifiers = f.getModifiers();
            if (!Modifier.isTransient(modifiers) &&
                    Modifier.isPublic(modifiers) &&
                    !Modifier.isStatic(modifiers)) {
                try {
                    Object o = f.get(proc);
                    if (o instanceof SQLStmt) {
                        stmts.put(f.getName(), (SQLStmt) o);
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to retrieve " + f + " from " + c.getSimpleName(), ex);
                }
            }
        }
        return (stmts);
    }

    @Override
    public String toString() {
        return (this.procName);
    }

    /**
     * Thrown from a Procedure to indicate to the Worker
     * that the procedure should be aborted and rolled back.
     */
    public static class UserAbortException extends RuntimeException {
        private static final long serialVersionUID = -1L;

        /**
         * Default Constructor
         *
         * @param msg
         * @param ex
         */
        public UserAbortException(String msg, Throwable ex) {
            super(msg, ex);
        }

        /**
         * Constructs a new UserAbortException
         * with the specified detail message.
         */
        public UserAbortException(String msg) {
            this(msg, null);
        }
    }
}
