# Perf Dataloader

### used to infer the table schema from the database and generate a benchbase interpreted input yaml file which can be used to load sample data into the table.


build command:
```
cd benchbase
./build.sh
```

how to use:
```
#$./perfloader --help
Usage: ./perfloader --config <config_file> --table-name <table_name> --rows <rows> [--generate-only] [--load-only]
Short forms: -c <config_file> -t <table_name> -r <rows>
Options:
  -c, --config       Configuration file
  -t, --table-name   Table name
  -r, --rows         Number of rows
  --generate-only    Only generate the loader file
  --load-only        Only load data into the database
  -h, --help         Display this help message

```
- to only generate the loader file(skip the actual load). This will generate the yaml file <table-name>_loader.yaml which can be used in loading the data.
```
./perfloader --config <config_file> --table-name <table_name> --rows <rows> --generate-only
```

- to only load the data(when your loader file is already generate)
```
./perfloader --config <config_file> --load-only
```

- to generate the loader yaml file and load the data in one go
```
./perfloader --config <config_file> --table-name <table_name> --rows <rows>
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