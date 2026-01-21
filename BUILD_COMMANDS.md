# BenchBase JAR Build Commands

This document contains the commands to generate benchbase.jar for both Java 17 and Java 23.

## Prerequisites

- Java 17 or Java 23 installed
- Maven (or use the included Maven wrapper `./mvnw`)

## Available Database Profiles

- `cockroachdb`
- `mariadb` 
- `mysql`
- `oracle`
- `phoenix`
- `postgres`
- `spanner`
- `sqlite`
- `sqlserver`

## Java 17 Build Commands

### Basic Build (with tests)
```bash
# For postgres profile (most common)
./mvnw clean package -P postgres -Djava.version=17 -Dmaven.compiler.source=17 -Dmaven.compiler.target=17

# For other profiles, replace 'postgres' with desired profile
./mvnw clean package -P mysql -Djava.version=17 -Dmaven.compiler.source=17 -Dmaven.compiler.target=17
```

### Optimized Build (without tests, faster)
```bash
# For postgres profile
./mvnw clean package verify -P postgres -DskipTests -Djava.version=17 -Dmaven.compiler.source=17 -Dmaven.compiler.target=17 -Ddescriptors=src/main/assembly/tgz.xml

# For other profiles
./mvnw clean package verify -P mysql -DskipTests -Djava.version=17 -Dmaven.compiler.source=17 -Dmaven.compiler.target=17 -Ddescriptors=src/main/assembly/tgz.xml
```

## Java 23 Build Commands

### Basic Build (with tests)
```bash
# For postgres profile (most common)
./mvnw clean package -P postgres

# For other profiles
./mvnw clean package -P mysql
```

### Optimized Build (without tests, faster)
```bash
# For postgres profile
./mvnw clean package verify -P postgres -DskipTests -Ddescriptors=src/main/assembly/tgz.xml

# For other profiles  
./mvnw clean package verify -P mysql -DskipTests -Ddescriptors=src/main/assembly/tgz.xml
```

## Multi-Profile Builds

### Build all profiles for Java 17
```bash
for profile in cockroachdb mariadb mysql oracle phoenix postgres spanner sqlite sqlserver; do
  ./mvnw clean package -P $profile -Djava.version=17 -Dmaven.compiler.source=17 -Dmaven.compiler.target=17
done
```

### Build all profiles for Java 23
```bash
for profile in cockroachdb mariadb mysql oracle phoenix postgres spanner sqlite sqlserver; do
  ./mvnw clean package -P $profile
done
```

## Output Location

After building, the artifacts will be in the `target/` directory:

1. **Archive**: `target/benchbase-<profile>.tgz`
2. **Extracted location**: `target/benchbase-<profile>/`
3. **JAR file**: `target/benchbase-<profile>/benchbase.jar`

## Extraction and Usage

```bash
# Navigate to target directory
cd target

# Extract the built archive
tar xvzf benchbase-postgres.tgz

# Navigate to extracted directory
cd benchbase-postgres

# Run BenchBase (show help)
java -jar benchbase.jar -h

# Example: Run TPCC benchmark
java -jar benchbase.jar -b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true --execute=true
```

## Alternative: Using the Build Script

You can also use the provided build script:

```bash
# Make executable (if not already)
chmod +x build-commands.sh

# Show help and all commands
./build-commands.sh

# Build Java 17 version with postgres profile
./build-commands.sh java17 postgres

# Build Java 23 version with mysql profile
./build-commands.sh java23 mysql

# Build optimized version
./build-commands.sh optimized postgres 17  # Java 17
./build-commands.sh optimized postgres 23  # Java 23
```

## Custom Plugin Configuration

After building, you can now specify a custom plugin.xml file using the `--plugin-config` parameter:

```bash
# Using custom plugin configuration
java -jar benchbase.jar --plugin-config /path/to/custom-plugin.xml -b mybench -c config.xml --execute=true

# Using relative path for custom plugins
java -jar benchbase.jar --plugin-config custom-plugins.xml -b tpcc -c config/postgres/sample_tpcc_config.xml --execute=true

# Show help with custom plugin config
java -jar benchbase.jar --plugin-config /path/to/custom-plugin.xml -h
```

## Custom Plugin.xml Format

Your custom plugin.xml should follow this structure:
```xml
<?xml version="1.0"?>
<plugins>
    <plugin name="mybench">com.mycompany.MyCustomBenchmark</plugin>
    <plugin name="tpcc">com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark</plugin>
    <plugin name="tpch">com.oltpbenchmark.benchmarks.tpch.TPCHBenchmark</plugin>
    <!-- Add your custom benchmarks here -->
</plugins>
```

## Notes

- The project is currently configured for Java 23 by default in `pom.xml`
- For Java 17 builds, we override the compiler settings via Maven properties
- The `-DskipTests` flag speeds up builds by skipping test execution
- Different database profiles include specific JDBC drivers for each database
- The `postgres` profile is the most commonly used and recommended for general testing
- If no `--plugin-config` is specified, it defaults to `config/plugin.xml`