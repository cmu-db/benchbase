# Perf Dataloader

### Used to infer the table schema from the database and generate a benchbase interpreted input yaml file which can be used to load sample data into the table.
This tool is integrated inside benchbase so that the users don't have to install additional tools for using it. All existing functionalities from yugabyte/benchbase should work as it is.

### Build steps:
#### Pre-requisites
- java version 17 or higher is installed
- maven version 3.6 or higher is installed

#### build steps
The build steps will also validate if the pre-requisites are met.
```
cd benchbase
./build.sh
```

### How to use:
```
#$./perf-data-loader --help
Usage: ./perf-data-loader --config <config_file> --table-name <table_name> --rows <rows> [--gen-config-only] [--load-only]
Short forms: -c <config_file> -t <table_name> -r <rows>
Options:
  -c, --config                Configuration file
  -t, --table-name            Table name
  -r, --rows                  Number of rows
  --gen-config-only           Only generate the loader/config file
  --load-only                 Only load data into the database
  -h, --help                  Display this help message
```
- to only generate the loader file(skip the actual load). This will generate the yaml file <table-name>_loader.yaml which can be used in loading the data.
```
./perf-data-loader --config <config_file> --table-name <table_name> --rows <rows> --gen-config-only
```

- to only load the data(when your loader file is already generate)
```
./perf-data-loader --config <config_file> --load-only
```

- to generate the loader yaml file and load the data in one go
```
./perf-data-loader --config <config_file> --table-name <table_name> --rows <rows>
```

the input yaml file should have following content
```
type: YUGABYTE
driver: com.yugabyte.Driver
url: jdbc:yugabytedb://localhost:5433/yugabyte?sslmode=require&reWriteBatchedInserts=true
username: yugabyte
password: password

tablename: {{tableName}}
rows: {{rows}}

```

#### Caveat/In-progress items
- partitioned tables are not yet supported.
- columns with user defined data types are not yet supported.

#### [Reference utility functions](https://github.com/yugabyte/benchbase/blob/main/src/main/java/com/oltpbenchmark/benchmarks/featurebench/Readme.md#utility-functions-)
