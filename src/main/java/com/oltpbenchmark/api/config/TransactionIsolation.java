package com.oltpbenchmark.api.config;

import java.sql.Connection;

public enum TransactionIsolation {
    TRANSACTION_SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE), TRANSACTION_READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED), TRANSACTION_REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ), TRANSACTION_READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED);

    private final int jdbcValue;

    TransactionIsolation(int jdbcValue) {
        this.jdbcValue = jdbcValue;
    }

    public int getJdbcValue() {
        return jdbcValue;
    }
}
