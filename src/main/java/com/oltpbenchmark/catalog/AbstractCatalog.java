package com.oltpbenchmark.catalog;

import java.sql.SQLException;
import java.util.Collection;

/**
 * An abstraction over a database's catalog.
 *
 * Concretely, this abstraction supports two types of catalogs:
 * - "real" catalogs, which query the table directly;
 * - catalogs backed by an in-memory instance of HSQLDB, in case the DBMS is unable to support certain SQL queries.
 */
public interface AbstractCatalog {
    Collection<Table> getTables();
    Table getTable(String tableName);
    void close() throws SQLException;
}
