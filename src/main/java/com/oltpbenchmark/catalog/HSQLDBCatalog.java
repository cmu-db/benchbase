package com.oltpbenchmark.catalog;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.SortDirectionType;
import com.oltpbenchmark.util.Pair;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HSQLDBCatalog implements AbstractCatalog {

    private static final String DB_CONNECTION = "jdbc:hsqldb:mem:";
    private static final String DB_JDBC = "org.hsqldb.jdbcDriver";
    private static final DatabaseType DB_TYPE = DatabaseType.HSQLDB;

    private final BenchmarkModule benchmarkModule;

    private final Map<String, Table> tables = new HashMap<>(); // original table name -> table
    private final Map<String, String> originalTableNames; // HSQLDB uppercase table name -> original table name

    private static final Random rand = new Random();

    /**
     * Connection to the HSQLDB instance.
     */
    private final Connection conn;

    public HSQLDBCatalog(BenchmarkModule benchmarkModule) {
        this.benchmarkModule = benchmarkModule;
        String dbName = String.format("catalog-%s-%d.db", this.benchmarkModule.getBenchmarkName(), rand.nextInt());

        Connection conn;
        try {
            Class.forName(DB_JDBC);
            conn = DriverManager.getConnection(DB_CONNECTION + dbName + ";sql.syntax_mys=true", null, null);
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
        this.conn = conn;

        this.originalTableNames = this.getOriginalTableNames();
        try {
            this.init();
        } catch (SQLException | IOException e) {
            throw new RuntimeException(String.format("Failed to initialize %s database catalog.", this.benchmarkModule.getBenchmarkName()), e);
        }
    }

    @Override
    public void close() throws SQLException {
        this.conn.close();
    }

    @Override
    public Collection<Table> getTables() {
        return tables.values();
    }

    @Override
    public Table getTable(String tableName) {
        return tables.get(originalTableNames.get(tableName.toUpperCase()));
    }

    private void init() throws SQLException, IOException {
        // Load the database DDL.
        this.benchmarkModule.createDatabase(DB_TYPE, this.conn);

        // TableName -> ColumnName -> <FKeyTable, FKeyColumn>
        Map<String, Map<String, Pair<String, String>>> foreignKeys = new HashMap<>();

        DatabaseMetaData md = conn.getMetaData();
        ResultSet tableRS = md.getTables(null, null, null, new String[]{"TABLE"});
        while (tableRS.next()) {
            String internalTableName = tableRS.getString(3);
            String upperTableName = internalTableName.toUpperCase();
            String originalTableName = originalTableNames.get(upperTableName);

            String tableType = tableRS.getString(4);
            if (!tableType.equalsIgnoreCase("TABLE")) continue;

            Table catalogTable = new Table(originalTableName, "");

            // COLUMNS
            try (ResultSet colRS = md.getColumns(null, null, internalTableName, null)) {
                while (colRS.next()) {
                    String colName = colRS.getString(4);
                    int colType = colRS.getInt(5);
                    String colTypeName = colRS.getString(6);
                    Integer colSize = colRS.getInt(7);
                    boolean colNullable = colRS.getString(18).equalsIgnoreCase("YES");

                    Column catalogCol = new Column(colName, "", catalogTable, colType, colSize, colNullable);
                    // TODO(WAN): The following block of code was relevant for programmatic CreateDialect support.
                    //            i.e., using the HSQLDB catalog instance to automatically create dialects for other DBMSs.
                    //            Since we don't add new database support often, and can hand-write most of that,
                    //            it is probably worth the tradeoff to have this functionality removed.
                    /*
                    {
                        String colDefaultValue = colRS.getString(13);
                        // TODO(WAN): Inherited FIXME autoinc should use colRS.getString(22).toUpperCase().equals("YES")
                        boolean colAutoInc = false;
                        catalogCol.setDefaultValue(colDefaultValue);
                        catalogCol.setAutoInc(colAutoInc);
                        catalogCol.setNullable(colNullable);
                        // TODO(WAN): Inherited FIXME setSigned
                    }
                    */

                    catalogTable.addColumn(catalogCol);
                }
            }

            // TODO(WAN): It looks like the primaryKeyColumns were only used in CreateDialect.
            /*
            {
                // PRIMARY KEYS
                try (ResultSet pkeyRS = md.getPrimaryKeys(null, null, internalTableName)) {
                    SortedMap<Integer, String> pkeyCols = new TreeMap<>();
                    while (pkeyRS.next()) {
                        String colName = pkeyRS.getString(4);
                        int colIdx = pkeyRS.getShort(5);
                        // TODO(WAN): Is this hack still necessary?
                        //            Previously, the index hack is around SQLite not returning the KEY_SEQ.
                        if (colIdx == 0) colIdx = pkeyCols.size();
                        pkeyCols.put(colIdx, colName);
                    }
                }
                catalogTable.setPrimaryKeyColumns(pkeyCols.values());
            }
            */

            // INDEXES
            try (ResultSet idxRS = md.getIndexInfo(null, null, internalTableName, false, false)) {
                while (idxRS.next()) {
                    int idxType = idxRS.getShort(7);
                    if (idxType == DatabaseMetaData.tableIndexStatistic) {
                        continue;
                    }
                    boolean idxUnique = !idxRS.getBoolean(4);
                    String idxName = idxRS.getString(6);
                    int idxColPos = idxRS.getInt(8) - 1;
                    String idxColName = idxRS.getString(9);
                    String sort = idxRS.getString(10);
                    SortDirectionType idxDirection;
                    if (sort != null) {
                        idxDirection = sort.equalsIgnoreCase("A") ? SortDirectionType.ASC : SortDirectionType.DESC;
                    } else {
                        idxDirection = null;
                    }

                    Index catalogIdx = catalogTable.getIndex(idxName);
                    if (catalogIdx == null) {
                        catalogIdx = new Index(idxName, "", catalogTable, idxType, idxUnique);
                        catalogTable.addIndex(catalogIdx);
                    }
                    catalogIdx.addColumn(idxColName, idxDirection, idxColPos);
                }
            }

            // FOREIGN KEYS
            try (ResultSet fkRS = md.getImportedKeys(null, null, internalTableName)) {
                foreignKeys.put(originalTableName, new HashMap<>());
                while (fkRS.next()) {
                    String colName = fkRS.getString(8);
                    String fkTableName = originalTableNames.get(fkRS.getString(3).toUpperCase());
                    String fkColName = fkRS.getString(4);
                    foreignKeys.get(originalTableName).put(colName, Pair.of(fkTableName, fkColName));
                }
            }

            // Register the table to the catalog.
            this.tables.put(originalTableName, catalogTable);
        }

        for (Table catalogTable : this.tables.values()) {
            Map<String, Pair<String, String>> fk = foreignKeys.get(catalogTable.getName());
            fk.forEach((colName, fkey) -> {
                Column catalogCol = catalogTable.getColumnByName(colName);

                Table fkeyTable = this.tables.get(fkey.first);
                if (fkeyTable == null) {
                    throw new RuntimeException("Unexpected foreign key parent table " + fkey);
                }

                Column fkeyCol = fkeyTable.getColumnByName(fkey.second);
                if (fkeyCol == null) {
                    throw new RuntimeException("Unexpected foreign key parent column " + fkey);
                }

                catalogCol.setForeignKey(fkeyCol);
            });
        }
    }

    /**
     * HACK:    HSQLDB will always uppercase table names.
     * The original table names are extracted from the DDL.
     *
     * @return A map from the original table names to the uppercase HSQLDB table names.
     */
    Map<String, String> getOriginalTableNames() {
        // Get the contents of the HSQLDB DDL for the current benchmark.
        String ddlContents;
        try {
            String ddlPath = this.benchmarkModule.getWorkloadConfiguration().getDDLPath();
            URL ddlURL;
            if (ddlPath == null) {
                ddlPath = this.benchmarkModule.getDatabaseDDLPath(DatabaseType.HSQLDB);
                ddlURL = Objects.requireNonNull(this.getClass().getResource(ddlPath));
            } else {
                ddlURL = Path.of(ddlPath).toUri().toURL();
            }
            ddlContents = IOUtils.toString(ddlURL, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Extract and map the original table names to their uppercase versions.
        Map<String, String> originalTableNames = new HashMap<>();
        Pattern p = Pattern.compile("CREATE[\\s]+TABLE[\\s]+(.*?)[\\s]+", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(ddlContents);
        while (m.find()) {
            String tableName = m.group(1).trim();
            originalTableNames.put(tableName.toUpperCase(), tableName);
        }
        return originalTableNames;
    }
}
