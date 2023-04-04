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

package com.oltpbenchmark.jdbc;

import com.oltpbenchmark.types.DatabaseType;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

public class AutoIncrementPreparedStatement implements PreparedStatement {

    private final DatabaseType dbType;
    private final PreparedStatement stmt;

    public AutoIncrementPreparedStatement(DatabaseType dbType, PreparedStatement stmt) {
        this.dbType = dbType;
        this.stmt = stmt;
    }

    /**
     * Special override for Postgres
     */
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        if (this.dbType == DatabaseType.POSTGRES
            || this.dbType == DatabaseType.COCKROACHDB
            || this.dbType == DatabaseType.SQLSERVER
            || this.dbType == DatabaseType.SQLAZURE
        ) {
            return this.stmt.getResultSet();
        } else {
            return this.stmt.getGeneratedKeys();
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return this.stmt.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return this.stmt.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        this.stmt.close();

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return this.stmt.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        this.stmt.setMaxFieldSize(max);

    }

    @Override
    public int getMaxRows() throws SQLException {
        return this.stmt.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.stmt.setMaxRows(max);

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        this.stmt.setEscapeProcessing(enable);

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return this.stmt.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.stmt.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        this.stmt.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.stmt.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.stmt.clearWarnings();

    }

    @Override
    public void setCursorName(String name) throws SQLException {
        this.stmt.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return this.stmt.execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.stmt.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return this.stmt.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return this.stmt.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.stmt.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return this.stmt.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.stmt.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.stmt.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return this.stmt.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return this.stmt.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        this.stmt.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        this.stmt.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return this.stmt.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.stmt.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return this.stmt.getMoreResults(current);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return this.stmt.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return this.stmt.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return this.stmt.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return this.stmt.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return this.stmt.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return this.stmt.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return this.stmt.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.stmt.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        this.stmt.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return this.stmt.isPoolable();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.stmt.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.stmt.isWrapperFor(iface);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return this.stmt.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return this.stmt.executeUpdate();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.stmt.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.stmt.setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.stmt.setByte(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.stmt.setShort(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.stmt.setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.stmt.setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.stmt.setFloat(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.stmt.setDouble(parameterIndex, x);

    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.stmt.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.stmt.setString(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.stmt.setBytes(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.stmt.setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.stmt.setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.stmt.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.stmt.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.stmt.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        this.stmt.clearParameters();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.stmt.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.stmt.setObject(parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        return this.stmt.execute();
    }

    @Override
    public void addBatch() throws SQLException {
        this.stmt.addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        this.stmt.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        this.stmt.setRef(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.stmt.setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.stmt.setClob(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        this.stmt.setArray(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.stmt.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.stmt.setDate(parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.stmt.setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.stmt.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.stmt.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.stmt.setURL(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return this.stmt.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        this.stmt.setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        this.stmt.setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        this.stmt.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.stmt.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.stmt.setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        this.stmt.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.stmt.setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.stmt.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.stmt.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.stmt.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.stmt.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        this.stmt.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        this.stmt.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        this.stmt.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        this.stmt.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        this.stmt.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.stmt.setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.stmt.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        this.stmt.setNClob(parameterIndex, reader);
    }

    // Java7 Fixes
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


}
