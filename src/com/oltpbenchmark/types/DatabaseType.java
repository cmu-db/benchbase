package com.oltpbenchmark.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum DatabaseType {

    DB2,
    MYSQL,
    POSTGRES,
    ORACLE,
    SQLSERVER,
    SQLITE,
    AMAZONRDS,
    HSTORE,
    SQLAZURE,
    ASSCLOWN,
    HSQLDB,
    H2,
    NUODB
    ;
    
    public static final DatabaseType TEST_TYPE = DatabaseType.HSQLDB; 
    
    protected static final Map<Integer, DatabaseType> idx_lookup = new HashMap<Integer, DatabaseType>();
    protected static final Map<String, DatabaseType> name_lookup = new HashMap<String, DatabaseType>();
    static {
        for (DatabaseType vt : EnumSet.allOf(DatabaseType.class)) {
            DatabaseType.idx_lookup.put(vt.ordinal(), vt);
            DatabaseType.name_lookup.put(vt.name().toUpperCase(), vt);
        }
    }
    
    public static DatabaseType get(String name) {
        DatabaseType ret = DatabaseType.name_lookup.get(name.toUpperCase());
        return (ret);
    }
}
