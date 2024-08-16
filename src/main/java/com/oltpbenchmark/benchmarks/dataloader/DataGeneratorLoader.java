package com.oltpbenchmark.benchmarks.dataloader;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.util.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.nodes.Tag;


import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DataGeneratorLoader extends Loader<DataGenerator> {
    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorLoader.class);

    private final Map<String, PropertyMapping> properties;
    private final Map<String, FkPropertyMapping> fkProperties;

    private final Map<String, PropertyMapping> pkProperties;
    private int minLevel = Integer.MAX_VALUE;

    public DataGeneratorLoader(DataGenerator benchmark, Map<String, PropertyMapping> properties,
                               Map<String, PropertyMapping> pkProperties, Map<String, FkPropertyMapping> fkProperties) {
        super(benchmark);
        this.properties = properties;
        this.fkProperties = fkProperties;
        this.pkProperties = pkProperties;
    }

    public static void checkIfTableExists(String tableName, Connection connection) throws SQLException {
        boolean exists = false;
        ResultSet resultSet = null;

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getTables(null, null, tableName, new String[]{"TABLE"});
            if (resultSet.next()) {
                exists = true;
            }
            if (!exists) {
                throw new RuntimeException(String.format("Table with name %s does not exist", tableName));
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }

    }

    public static List<Column> getTableSchema(String tableName, Connection conn) {
        List<Column> tableSchemaList = new ArrayList<>();
        String query = "SELECT column_name, data_type, character_maximum_length, is_identity " +
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {

            pstmt.setString(1, tableName);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                Column column = new Column();
                column.setColumnName(rs.getString("column_name"));
                // need to remove all spaces from datatype so that we can do string matching with properties
                column.setDataType(rs.getString("data_type").replaceAll("\\s", ""));
                column.setCharacterMaximumLength((Integer) rs.getObject("character_maximum_length"));
                column.setIsIdentity("YES".equals(rs.getString("is_identity")));
                tableSchemaList.add(column);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return tableSchemaList;
    }

    public static List<PrimaryKey> getPrimaryKeys(String tableName, Connection conn) {
        List<PrimaryKey> primaryKeyList = new ArrayList<>();
        String query = "SELECT c.column_name, c.data_type " +
            "FROM information_schema.table_constraints tc " +
            "JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) " +
            "JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema " +
            "AND tc.table_name = c.table_name " +
            "AND ccu.column_name = c.column_name " +
            "WHERE constraint_type = 'PRIMARY KEY' " +
            "AND tc.table_name = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {

            pstmt.setString(1, tableName);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                PrimaryKey primaryKey = new PrimaryKey();
                primaryKey.setColumnName(rs.getString("column_name"));
                // need to remove all spaces from datatype so that we can do string matching with properties
                primaryKey.setDataType(rs.getString("data_type").replaceAll("\\s", ""));
                primaryKeyList.add(primaryKey);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return primaryKeyList;
    }

    public static List<String> getUniqueConstrains(String tableName, Connection conn) {
        List<String> uniques = new ArrayList<>();
        // Query to find unique indexes on 'store' table in 'public' schema
        String query = "SELECT indexdef " +
            "FROM pg_indexes " +
            "WHERE indexdef ILIKE '%UNIQUE%' " +
            "AND tablename = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {

            pstmt.setString(1, tableName);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                String indexDef = rs.getString("indexdef");
                List<String> columns = extractColumnsFromIndexDef(indexDef);

                uniques.addAll(columns);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return uniques;
    }

    public static Map<String, List<Object>> getUserDefinedEnumDataTypes(String tableName, String schemaName, Connection conn) {
        String query =
            "SELECT " +
                "    a.attname AS column_name, " +
                "    t.typname AS data_type " +
                "FROM " +
                "    pg_attribute a " +
                "JOIN " +
                "    pg_class c ON a.attrelid = c.oid " +
                "JOIN " +
                "    pg_type t ON a.atttypid = t.oid " +
                "JOIN " +
                "    pg_namespace n ON t.typnamespace = n.oid " +
                "WHERE " +
                "    c.relname = ? AND " +
                "    n.nspname = ? AND " +
                "    (t.typtype = 'c' OR t.typtype = 'e' OR t.typtype = 'b') " +
                "ORDER BY " +
                "    a.attnum;";

        Map<String, List<Object>> columnDataTypes = new HashMap<>();

        try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
            preparedStatement.setString(1, tableName);
            preparedStatement.setString(2, schemaName);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString("column_name");
                    String dataType = resultSet.getString("data_type");
                    List<Object> distinctEnumValues = getEnumValues(dataType, conn);
                    columnDataTypes.put(columnName, distinctEnumValues);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return columnDataTypes;
    }

    public static List<Object> getEnumValues(String typename, Connection conn) throws SQLException {
        String query =
            "SELECT e.enumlabel AS enum_value " +
                "FROM pg_enum e " +
                "JOIN pg_type t ON t.oid = e.enumtypid " +
                "WHERE t.typname = ? " +
                "ORDER BY e.enumsortorder";

        List<Object> enumValues = new ArrayList<>();

        try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
            preparedStatement.setString(1, typename);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    enumValues.add(resultSet.getString("enum_value"));
                }
            }
        }

        return enumValues;
    }

    // Method to extract column name(s) from index definition
    private static List<String> extractColumnsFromIndexDef(String indexDef) {
        // Example index definition: "CREATE UNIQUE INDEX idx_name ON tablename (column1, column2)"
        // Extract column names from the parenthesis using regex
        // Assuming the format is generally consistent, using regex to find column names in parentheses
        String regex = "\\(([^)]+)\\)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(indexDef);
        List<String> filteredColumns = new ArrayList<>();

        if (matcher.find()) {
            String columnList = matcher.group(1);
            // Split by comma and trim to get individual column names
            String[] columns = columnList.split("\\s*,\\s*");
            // Remove non-column words (e.g., HASH) by filtering out any non-alphabetic words
            for (String column : columns) {
                String[] splits = column.split(" ");
                filteredColumns.add(splits[0]);
            }
        }
        return filteredColumns;
    }

    public static List<ForeignKey> getForeignKeys(String tableName, Connection conn) {
        List<ForeignKey> foreignKeyList = new ArrayList<>();
        String query = "SELECT " +
            "kcu.table_schema AS schema_name, " +
            "kcu.table_name AS table_name, " +
            "kcu.column_name AS column_name, " +
            "col.data_type AS column_data_type, " +
            "ccu.table_schema AS foreign_table_schema, " +
            "ccu.table_name AS foreign_table_name, " +
            "ccu.column_name AS foreign_column_name " +
            "FROM information_schema.table_constraints AS tc " +
            "JOIN information_schema.key_column_usage AS kcu " +
            "ON tc.constraint_name = kcu.constraint_name " +
            "AND tc.table_schema = kcu.table_schema " +
            "JOIN information_schema.constraint_column_usage AS ccu " +
            "ON ccu.constraint_name = tc.constraint_name " +
            "JOIN information_schema.columns AS col " +
            "ON kcu.table_schema = col.table_schema " +
            "AND kcu.table_name = col.table_name " +
            "AND kcu.column_name = col.column_name " +
            "WHERE tc.constraint_type = 'FOREIGN KEY' " +
            "AND kcu.table_name = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {

            pstmt.setString(1, tableName);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                ForeignKey foreignKey = new ForeignKey();
                foreignKey.setSchemaName(rs.getString("schema_name"));
                foreignKey.setTableName(rs.getString("table_name"));
                foreignKey.setColumnName(rs.getString("column_name"));
                // need to remove all spaces from datatype so that we can do string matching with properties
                foreignKey.setColumnDataType(rs.getString("column_data_type").replaceAll("\\s", ""));
                foreignKey.setForeignTableSchema(rs.getString("foreign_table_schema"));
                foreignKey.setForeignTableName(rs.getString("foreign_table_name"));
                foreignKey.setForeignColumnName(rs.getString("foreign_column_name"));
                foreignKeyList.add(foreignKey);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return foreignKeyList;
    }

    public static void buildDependencyDAGForTable(Map<String, List<Dependency>> graph, List<ForeignKey> foreignKeyList, Connection conn) {
        // Build the adjacency list
        for (ForeignKey fk : foreignKeyList) {
            graph.computeIfAbsent(fk.getTableName(), k -> new ArrayList<>()).add(new Dependency(fk.getForeignTableName(), -1));
            List<ForeignKey> fkOfFks = getForeignKeys(fk.getForeignTableName(), conn);
            if (!fkOfFks.isEmpty()) {
                buildDependencyDAGForTable(graph, fkOfFks, conn);
            }
        }
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        Connection conn = benchmark.makeConnection();
        boolean genLoadOrderOnly = workConf.getXmlConfig().getBoolean("gen-db-load-order", false);
        if (genLoadOrderOnly) {
            Set<String> processedTables = new HashSet<>();
            Map<String, List<Dependency>> graph = new HashMap<>();
            Set<String> visited = new HashSet<>();
            StringBuilder loadOrder = new StringBuilder();
            Map<Integer, List<String>> depth = new TreeMap<>();
            Map<Integer, List<String>> levelAndTables = new LinkedHashMap<>();
            buildGraph(conn, processedTables, graph);

            List<String> independentTables = new ArrayList<>();
            List<String> allDbTables = getAllTables(conn);
            for(String table: allDbTables) {
                if (!processedTables.contains(table.toLowerCase())) {
                    independentTables.add(table);
                }
            }

            if (!graph.isEmpty()) {
                String startTable = graph.keySet().iterator().next();
                levelAndTables.putAll(getOrderOfImport(startTable, loadOrder, graph, depth, visited));
                levelAndTables.get(0).addAll(independentTables);
            } else {
                levelAndTables.put(0, independentTables);
            }

            int totalTables = 0;
            for (Map.Entry<Integer, List<String>> entry : levelAndTables.entrySet()) {
                int level = entry.getKey();
                List<String> tablesAtLevel = entry.getValue();
                totalTables += tablesAtLevel.size();
                System.out.println("Level " + level + ": " + String.join(", ", tablesAtLevel));
            }

            System.out.println("Total number of tables: " + totalTables);
            try {
                FileWriter writer = new FileWriter("load_order.json");
                writer.write(JSONUtil.format(JSONUtil.toJSONString(levelAndTables)));
                writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } else {
            String tableName = workConf.getXmlConfig().getString("tablename");
            int rows = workConf.getXmlConfig().getInt("rows");

            // check if the table exists in the database
            checkIfTableExists(tableName, conn);
            // get the table schema
            List<Column> tableSchema = getTableSchema(tableName, conn);

            // key primary key details
            List<PrimaryKey> primaryKeys = getPrimaryKeys(tableName, conn);

            // get all unique constraints from the indexes
            List<String> uniqueConstraintColumns = getUniqueConstrains(tableName, conn);

            // get all columns with respective user defined ENUM data type
            Map<String, List<Object>> udColumns = getUserDefinedEnumDataTypes(tableName, "public", conn);

            // get all foreign keys of the table
            List<ForeignKey> foreignKeys = getForeignKeys(tableName, conn);
//        System.out.println(foreignKeys);


            int limit = Math.min(10000, rows);
            List<String> fkColNames = new ArrayList<>();

            // if in foreign key, parent table is same as current table, don't treat it as foreign key. treat is as normal column
            List<ForeignKey> fkToRemove = new ArrayList<>();
            foreignKeys.forEach(fk -> {
                if (fk.getForeignTableName().equalsIgnoreCase(tableName)) {
                    fkToRemove.add(fk);
                } else {
                    fkColNames.add(fk.getColumnName());
                }
            });
            foreignKeys.removeAll(fkToRemove);

            // remove all fks from unique constraints
            uniqueConstraintColumns.removeAll(fkColNames);

            // remove all fks from primary keys
            List<PrimaryKey> pkToRemove = new ArrayList<>();
            primaryKeys.forEach(pk -> {
                if (fkColNames.contains(pk.getColumnName()))
                    pkToRemove.add(pk);
            });
            primaryKeys.removeAll(pkToRemove);

            if (!foreignKeys.isEmpty()) {
                // fetch the distinct values from parent table. This could take some time
                getDistinctValuesFromParentTable(conn, foreignKeys, limit);
            }
            // create mapping of utility function to the columns in the table
            Map<String, PropertyMapping> columnToUtilsMapping =
                utilsMapping(tableSchema, primaryKeys, foreignKeys, limit, rows, uniqueConstraintColumns, udColumns);

            // generate the mapping object which can be used to create the output yaml file
            Root root = generateMappingObject(tableName, rows, columnToUtilsMapping, fkColNames, udColumns);

            // create output yaml file
            writeToFile(tableName, rows, root);
            LOG.info("Generated loader file: {}_loader.yaml", tableName);
        }

        return new ArrayList<>();
    }

    public Map<String, PropertyMapping> utilsMapping(List<Column> tableSchema, List<PrimaryKey> primaryKeys,
                                                     List<ForeignKey> foreignKeys, int limit, int rows,
                                                     List<String> uniqueConstraintColumns,
                                                     Map<String, List<Object>> udColumData) {
        Map<String, PropertyMapping> columnToUtilMapping = new LinkedHashMap<>();
        // take care of the primary keys first
        for (PrimaryKey pk : primaryKeys) {
            columnToUtilMapping.put(pk.getColumnName(), pkProperties.get(pk.getDataType()));
        }

        for (ForeignKey fk : foreignKeys) {
            if (!columnToUtilMapping.containsKey(fk.getColumnName())) {
                FkPropertyMapping fkm = fkProperties.get(fk.getColumnDataType());
                PropertyMapping pm = new PropertyMapping(fkm.className, limit, fk.getDistinctValues());
                columnToUtilMapping.put(fk.getColumnName(), pm);
            }
        }
        udColumData.forEach((colName, values) -> {
            if (!columnToUtilMapping.containsKey(colName)) {
                // Caveat: treating all user defined data types as list of String utility
                PropertyMapping pm = new PropertyMapping("OneStringFromArray", values.size(), values);
                columnToUtilMapping.put(colName, pm);
            }
        });
        // take care of the rest of the keys
        for (Column col : tableSchema) {
            if (!columnToUtilMapping.containsKey(col.getColumnName())) {
                PropertyMapping pm;
                // if column is one of the unique constraint columns, then use primary key util functions for it
                if (uniqueConstraintColumns.contains(col.getColumnName()))
                    pm = pkProperties.get(col.getDataType());
                else
                    pm = properties.get(col.getDataType().toLowerCase());

                if (pm == null) {
                    throw new RuntimeException(String.format("Cannot find suitable utility function for column " +
                        "`%s` of datatype `%s`. Consider asking #perf team to add a utility function for given " +
                        "data type", col.getColumnName(), col.getDataType()));
                }
                PropertyMapping pmForColumn = new PropertyMapping(pm);
                for (int i = 0; i < pmForColumn.params.size(); i++) {
                    Object obj = pmForColumn.params.get(i);
                    if (obj instanceof String) {
                        if (obj.toString().equalsIgnoreCase("rows")) {
                            pmForColumn.params.set(i, rows);
                        } else if (obj.toString().equalsIgnoreCase("max") && col.getCharacterMaximumLength() != null) {
                            pmForColumn.params.set(i, col.getCharacterMaximumLength());
                        }
                    }
                }
                columnToUtilMapping.put(col.getColumnName(), pmForColumn);
            }
        }

        return columnToUtilMapping;
    }

    public Root generateMappingObject(String tableName, int rows, Map<String, PropertyMapping> colToUtilsMapping,
                                      List<String> fkColNames, Map<String, List<Object>> udColumns) {
        LoadRule loadRule = new LoadRule(tableName, rows);
        colToUtilsMapping.forEach((colName, prop) -> {
            Column1 col = new Column1(colName, prop.className);

            prop.params.forEach(param -> {
                if (fkColNames.contains(colName)) {
                    if (param instanceof Integer)
                        col.params.add(param);
                    else
                        col.params.add(param.toString());
                } else if (udColumns.containsKey(colName)) {
                    if (param instanceof Integer)
                        col.params.add(param);
                    else
                        col.params.add(param.toString());
                } else {
                    if (param.toString().equalsIgnoreCase("rows")) {
                        col.params.add(rows);
                    } else {
                        col.params.add(Integer.parseInt(param.toString()));
                    }
                }
            });

            loadRule.columns.add(col);
        });
        Properties properties1 = new Properties(List.of(loadRule));
        Microbenchmark mb = new Microbenchmark(properties1);
        return new Root(mb);
    }

    public void writeToFile(String tableName, int rows, Root root) {
        // Configure SnakeYAML options
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);

        Representer representer = new Representer(options);
        representer.addClassTag(Root.class, Tag.MAP);
        representer.setPropertyUtils(new CustomPropertyUtils());
        Yaml yaml = new Yaml(representer, options);

        try {
            // Read existing YAML file
            FileReader reader = new FileReader(workConf.getConfigFilePath());
            Map<String, Object> existingData = yaml.load(reader);
            reader.close();

            // Create new YAML content
            StringWriter writer = new StringWriter();
            yaml.dump(root, writer);
            Map<String, Object> newData = yaml.load(new StringReader(writer.toString()));

            // Create a new combined data map
            Map<String, Object> combinedData = new LinkedHashMap<>();
            combinedData.putAll(existingData);
            combinedData.putAll(newData);

            // Write combined data to the new output file
            FileWriter fileWriter = new FileWriter(String.format("%s/%s_loader.yaml", System.getProperty("user.dir"), tableName));
            yaml.dump(combinedData, fileWriter);
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getDistinctValuesFromParentTable(Connection conn, List<ForeignKey> foreignKeyList, int limit) {
        String queryTemplate = "SELECT DISTINCT %s FROM %s LIMIT %d";
        for (ForeignKey foreignKey : foreignKeyList) {
            String query = String.format(queryTemplate, foreignKey.getForeignColumnName(), foreignKey.getForeignTableName(), limit);
            try (PreparedStatement pstmt = conn.prepareStatement(query);
                 ResultSet rs = pstmt.executeQuery()) {

                List<Object> distinctValues = new ArrayList<>();
                while (rs.next()) {
                    distinctValues.add(rs.getObject(1));
                }
                if (distinctValues.isEmpty()) {
                    Map<Integer, String> levelAndTables = new LinkedHashMap<>();
                    Map<String, List<Dependency>> graph = new HashMap<>();
                    Map<Integer, List<String>> depth = new TreeMap<>();
                    Set<String> visited = new HashSet<>();

                    buildDependencyDAGForTable(graph, foreignKeyList, conn);

                    StringBuilder loadOrder = new StringBuilder(String.format("There are no entries in the parent " +
                            "table `%s` for column `%s` to be used as foreign key. Consider loading tables in " +
                            "following order/Levels(tables from `Level 0` first, then `Level 1` and so on: ", foreignKey.getForeignTableName(),
                        foreignKey.getForeignColumnName()));

                    String startTable = graph.keySet().iterator().next();
                    Map<Integer, List<String>> levelsAndTables = getOrderOfImport(startTable, loadOrder, graph, depth, visited);
                    throw new RuntimeException(loadOrder.append(generateLoadOrder(levelsAndTables)).toString());
                }
                foreignKey.setDistinctValues(distinctValues);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Column1 {
        public String name;
        public String util;
        public List<Object> params;

        public Column1(String name, String util) {
            this.name = name;
            this.util = util;
            this.params = new ArrayList<>();
        }
    }

    public static class LoadRule {
        public String table;
        public int rows;
        public List<Column1> columns = new ArrayList<>();

        public LoadRule(String table, int rows, List<Column1> columns) {
            this.table = table;
            this.rows = rows;
            this.columns = columns;
        }

        public LoadRule(String table, int rows) {
            this.table = table;
            this.rows = rows;
        }
    }

    public static class Properties {
        public List<LoadRule> loadRules;

        public Properties(List<LoadRule> loadRules) {
            this.loadRules = loadRules;
        }
    }

    public static class Microbenchmark {
        public Properties properties;
        @JsonProperty("class")
        public String className = "com.oltpbenchmark.benchmarks.featurebench.customworkload.YBDefaultMicroBenchmark";

        public Microbenchmark(Properties properties) {
            this.properties = properties;
        }
    }

    public static class Root {
        public Microbenchmark microbenchmark;

        public Root(Microbenchmark microbenchmark) {
            this.microbenchmark = microbenchmark;
        }
    }

    // Create custom PropertyUtils to enforce field order
    static class CustomPropertyUtils extends PropertyUtils {
        @Override
        protected Set<Property> createPropertySet(Class<?> type, BeanAccess bAccess) {
            Set<Property> properties = super.createPropertySet(type, bAccess);
            if (type == Column1.class) {
                List<String> order = Arrays.asList("name", "util", "params");
                List<Property> sortedProperties = new ArrayList<>(properties);
                sortedProperties.sort(Comparator.comparingInt(p -> order.indexOf(p.getName())));
                return new LinkedHashSet<>(sortedProperties);
            }
            if (type == Microbenchmark.class) {
                List<String> order = Arrays.asList("className", "properties");
                List<Property> sortedProperties = new ArrayList<>(properties);
                sortedProperties.sort(Comparator.comparingInt(p -> order.indexOf(p.getName())));
                return new LinkedHashSet<>(sortedProperties);
            }
            if (type == LoadRule.class) {
                List<String> order = Arrays.asList("table", "rows", "columns");
                List<Property> sortedProperties = new ArrayList<>(properties);
                sortedProperties.sort(Comparator.comparingInt(p -> order.indexOf(p.getName())));
                return new LinkedHashSet<>(sortedProperties);
            }
            return properties;
        }
    }

    // Helper class to store table name and level change
    public static class Dependency {
        String table;
        int levelChange;

        Dependency(String table, int levelChange) {
            this.table = table;
            this.levelChange = levelChange;
        }
    }

    // Function to print nodes by levels
    public Map<Integer, List<String>> getOrderOfImport(String startTable, StringBuilder loadOrder,
                              Map<String, List<Dependency>> graph , Map<Integer, List<String>> depth,
                              Set<String> visited) {
        Map<Integer, List<String>> levelsAndTables = new LinkedHashMap<>();
        dfs(startTable, 0, graph, depth, visited);

        // Adjust levels to start from 0
        Map<Integer, List<String>> adjustedDepth = new TreeMap<>();
        for (Map.Entry<Integer, List<String>> entry : depth.entrySet()) {
            int adjustedLevel = entry.getKey() - minLevel;
            adjustedDepth.computeIfAbsent(adjustedLevel, k -> new ArrayList<>()).addAll(entry.getValue());
        }

        // Output the final levels of nodes
        for (Map.Entry<Integer, List<String>> entry : adjustedDepth.entrySet()) {
            int level = entry.getKey();
            List<String> tablesAtLevel = entry.getValue();
            levelsAndTables.put(level, tablesAtLevel);
        }

        return levelsAndTables;
    }

    public StringBuilder generateLoadOrder(Map<Integer, List<String>> levelAndTables) {
        StringBuilder loadOrder = new StringBuilder();
        for (Map.Entry<Integer, List<String>> entry : levelAndTables.entrySet()) {
            int level = entry.getKey();
            loadOrder.append("\n").append("Level ").append(level).append(": ").append(String.join(", ", levelAndTables.get(level)));
        }

        return loadOrder;
    }

    // DFS function to populate the levels map
    public void dfs(String table, int level, Map<String, List<Dependency>> graph , Map<Integer, List<String>> depth,
                    Set<String> visited) {
        visited.add(table);
        depth.computeIfAbsent(level, k -> new ArrayList<>()).add(table);

        // Update the minimum level encountered
        if (level < minLevel) {
            minLevel = level;
        }

        if (graph.containsKey(table)) {
            for (Dependency dep : graph.get(table)) {
                if (!visited.contains(dep.table)) {
                    dfs(dep.table, level + dep.levelChange, graph, depth, visited);
                }
            }
        }
    }

    public void buildGraph(Connection conn, Set<String> processedTables, Map<String, List<Dependency>> graph) {
        String query = "SELECT tc.table_name AS child_table, ccu.table_name AS parent_table "
            + "FROM information_schema.table_constraints AS tc "
            + "JOIN information_schema.key_column_usage AS kcu "
            + "ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
            + "JOIN information_schema.constraint_column_usage AS ccu "
            + "ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema "
            + "WHERE tc.constraint_type = 'FOREIGN KEY'";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                String childTable = rs.getString("child_table");
                String parentTable = rs.getString("parent_table");

                processedTables.add(parentTable.toLowerCase());
                processedTables.add(childTable.toLowerCase());
                // Build the adjacency list
                graph.computeIfAbsent(parentTable, k -> new ArrayList<>()).add(new Dependency(childTable, 1));
                graph.computeIfAbsent(childTable, k -> new ArrayList<>()).add(new Dependency(parentTable, -1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getAllTables(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        String[] types = {"TABLE"};
        try (ResultSet rs = metaData.getTables(null, null, "%", types)) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                tables.add(tableName);
            }
        }
        return tables;
    }
}
