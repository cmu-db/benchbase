package com.oltpbenchmark.api.config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.oltpbenchmark.types.DatabaseType;

import java.sql.Driver;

public record Database(DatabaseType type, Class<? extends Driver> driverClass, String url, String username, String password, TransactionIsolation transactionIsolation, Integer batchSize) {
}
