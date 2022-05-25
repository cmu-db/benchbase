package com.oltpbenchmark.api.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oltpbenchmark.types.DatabaseType;

import java.sql.Driver;

@JacksonXmlRootElement(localName = "database")
@JsonIgnoreProperties(ignoreUnknown = true)
public record Database(DatabaseType type,
                       Class<? extends Driver> driverClass,
                       String url,
                       String username,
                       String password,
                       TransactionIsolation transactionIsolation,
                       Integer batchSize,
                       Integer retries) {
}
