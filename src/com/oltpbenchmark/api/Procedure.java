package com.oltpbenchmark.api;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public abstract class Procedure {
    private static final Logger LOG = Logger.getLogger(Procedure.class);

    private Map<String, SQLStmt> name_stmt_xref;
    private final Map<SQLStmt, String> stmt_name_xref = new HashMap<SQLStmt, String>();
    private final Map<SQLStmt, PreparedStatement> prepardStatements = new HashMap<SQLStmt, PreparedStatement>();
    
    /**
     * Mapping from SQLStmt to database-specific SQL
     */
    private final Map<SQLStmt, String> database_sql = new HashMap<SQLStmt, String>();

    /**
     * Constructor
     */
    protected Procedure() {
        // Nothing we can do here
    }
    
    /**
     * Initialize all of the SQLStmt handles. This must be called separately from
     * the constructor, otherwise we can't get access to all of our SQLStmts.
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    protected <T extends Procedure> T initialize() {
        this.name_stmt_xref = Procedure.getStatments(this);
        for (Entry<String, SQLStmt> e : this.name_stmt_xref.entrySet()) {
            this.stmt_name_xref.put(e.getValue(), e.getKey());
        } // FOR
        LOG.info(String.format("Initialized %s with %d SQLStmts: %s",
                               this, this.name_stmt_xref.size(), this.name_stmt_xref.keySet()));
        return ((T)this);
    }
    
    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt. 
     * @param conn
     * @param stmt
     * @param returnGeneratedKeys 
     * @return
     * @throws SQLException
     */
    public final PreparedStatement getPreparedStatement(Connection conn, SQLStmt stmt) throws SQLException {
        assert(this.name_stmt_xref != null) : "The Procedure " + this + " has not been initialized yet!";
        PreparedStatement pStmt = this.prepardStatements.get(stmt);
        if (pStmt == null) {
            assert(this.stmt_name_xref.containsKey(stmt)) :
                "Unexpected SQLStmt handle in " + this.getClass().getSimpleName() + "\n" + this.name_stmt_xref;
            String sql = this.database_sql.get(stmt);
            if (sql == null) sql = stmt.getSQL();
            pStmt = conn.prepareStatement(sql);
            this.prepardStatements.put(stmt, pStmt);
        }
        assert(pStmt != null) : "Unexpected null PreparedStatement for " + stmt;
        return (pStmt);
    }
    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt. 
     * @param conn
     * @param stmt
     * @param returnGeneratedKeys 
     * @return
     * @throws SQLException
     */
    public final PreparedStatement getPreparedStatement(Connection conn, SQLStmt stmt, int returnGeneratedKeys) throws SQLException {
        assert(this.name_stmt_xref != null) : "The Procedure " + this + " has not been initialized yet!";
        PreparedStatement pStmt = this.prepardStatements.get(stmt);
        if (pStmt == null) {
            assert(this.stmt_name_xref.containsKey(stmt)) :
                "Unexpected SQLStmt handle in " + this.getClass().getSimpleName() + "\n" + this.name_stmt_xref;
            String sql = this.database_sql.get(stmt);
            if (sql == null) sql = stmt.getSQL();
            pStmt = conn.prepareStatement(sql, returnGeneratedKeys);
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
        for (Entry<String, SQLStmt> e : this.name_stmt_xref.entrySet()) { 
            SQLStmt stmt = e.getValue();
            try {
                this.getPreparedStatement(conn, stmt);
            } catch (Throwable ex) {
                throw new RuntimeException(String.format("Failed to generate PreparedStatements for %s.%s", this, e.getKey()), ex);
            }
        } // FOR
    }
    
    protected final void setDatabaseSQL(String name, String sql) {
        SQLStmt stmt = this.name_stmt_xref.get(name);
        assert(stmt != null) : "Unexpected SQLStmt handle " + this.getClass().getSimpleName() + "." + name;
        this.database_sql.put(stmt, sql);
    }
    
    /**
     * Hook for testing
     * @return
     */
    protected Map<String, SQLStmt> getStatments() {
        return (Collections.unmodifiableMap(this.name_stmt_xref));
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
