package com.oltpbenchmark.api;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public abstract class Procedure {

    private final Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref;
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();
    
    /**
     * Mapping from SQLStmt to database-specific SQL
     */
    private final Map<SQLStmt, String> database_sql = new HashMap<SQLStmt, String>();

    public Procedure() {
        this.name_stmt_xref = Procedure.getStatments(this);
        this.stmt_name_xref = new HashMap<SQLStmt, String>();
        for (Entry<String, SQLStmt> e : this.name_stmt_xref.entrySet()) {
            this.stmt_name_xref.put(e.getValue(), e.getKey());
        } // FOR
    }
    
    public final PreparedStatement getPreparedStatement(Connection conn, SQLStmt stmt) throws SQLException {
        PreparedStatement pStmt = this.prepardStatements.get(stmt);
        if (pStmt == null) {
            String sql = this.database_sql.get(stmt);
            assert(sql != null) : "Unexpected SQLStmt handle " + this.getClass().getSimpleName() + "." + this.stmt_name_xref.get(stmt);
            pStmt = conn.prepareStatement(sql);
            this.prepardStatements.put(stmt, pStmt);
        }
        assert(pStmt != null) : "Unexpected null PreparedStatement for " + stmt;
        return (pStmt);
    }
    
    /**
     * Initialize all the PreparedStatements needed by this Procedure
     * @param conn
     * @throws SQLException
     */
    protected final void generateAllPreparedStatements(Connection conn) {
        for (SQLStmt stmt : this.stmt_name_xref.keySet()) {
            try {
                this.getPreparedStatement(conn, stmt);
            } catch (SQLException ex) {
                throw new RuntimeException(String.format("Failed to generate PreparedStatements for %s.%s", this, stmt), ex);
            }
        } // FOR
    }
    
    protected final void setDatabaseSQL(String name, String sql) {
        SQLStmt stmt = this.name_stmt_xref.get(name);
        assert(stmt != null) : "Unexpected SQLStmt handle " + this.getClass().getSimpleName() + "." + name;
        this.database_sql.put(stmt, sql);
    }
    
    protected static Map<String, SQLStmt> getStatments(Procedure proc) {
        Class<? extends Procedure> c = proc.getClass();
        Map<String, SQLStmt> stmts = new HashMap<String, SQLStmt>();
        for (Field f : c.getDeclaredFields()) {
            int modifiers = f.getModifiers();
            if (Modifier.isTransient(modifiers) == false &&
                Modifier.isPublic(modifiers) == true &&
                Modifier.isStatic(modifiers) == false) {
                try {
                    Object o = f.get(proc);
                    if (o instanceof SQLStmt) {
                        stmts.put(f.getName(), (SQLStmt)o);
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to retrieve " + f + " from " + c.getSimpleName(), ex);
                }
            }
        } // FOR
        return (stmts);
    }
    
    @Override
    public String toString() {
        return (this.getClass().getSimpleName());
    }
    
}
