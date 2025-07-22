##  Featurebench:-


### Flowchart
![Alt text](Flowchart.jpg?raw=true "Optional Title")

### Quickstart

To clone and build BenchBase using the `yugabyte` profile:-

```bash
git clone --depth 1 https://github.com/yugabyte/benchbase.git
cd benchbase
./mvnw clean package -P yugabyte
```

This produces artifacts in the `target` folder, which can be extracted:-

```bash
cd target
tar xvzf benchbase-yugabyte.tgz
cd benchbase-yugabyte
```

Inside this folder, you can run microbenchmarks using featurebench.

Eg:-

```bash
java -jar benchbase.jar -b featurebench -c config/yugabyte/demo/featurebench_santanu_t1_10_10k.yaml --create=true --load=true --execute=true
```


### Different ways of running :-

**Case 1**: When create, load and execute phase are taken from YAML.

Eg:-
```bash
java -jar benchbase.jar -b featurebench -c config/yugabyte/demo/featurebench_santanu_t1_10_10k.yaml --create=true --load=true --execute=true
```

**Case 2**: When the user writes the create, load and execute phases themseleves (overriding the create, loadOnce & executeOnce of YBMicrobenchmark class).

User has to include their own customworkload class extending abstract class YBMicroBenchmark.

In case of executeOnce the workload is executed only once(single threaded).

Eg:-
```bash
java -jar benchbase.jar -b featurebench -c config/yugabyte/demo/featurebench_scan_sonal.yaml --create=true --load=true --execute=true
```

**Case 3**:
When the user writes the create, load and execute phases themseleves (overriding the create, loadOnce & execute of YBMicrobenchmark class).

User has to include their own customworkload class extending abstract class YBMicroBenchmark.

In case of execute the workload is executed multiple times(multithreaded).
```bash
java -jar benchbase.jar -b featurebench -c config/yugabyte/demo/featurebench_microbenchmark1_sonal.yaml --create=true --load=true --execute=true
```

### YAML Config
| Key           | Description                                                                                                      |
|---------------|------------------------------------------------------------------------------------------------------------------|
| createdb      | To make databases with additional properties. DDL's for database to be written here(eg. for colocated property). |
 | create        | Has DDL's for create phase from YAML.                                                                            |
| loadRules     | Has key/value for table name, util and parameters for util functions.                                            |
| executeRules  | Has key/value for workload name and their details (parallel run names,weights and queries+bindings).             |
| cleanup       | Has DDL's for cleaning up tables from YAML.                                                                      |
| execute       | True/false (User writes their own execute Rules in execute() of customworkload class).                           |
| setAutoCommit | True/False                                                                                                       |


NOTE :-
1. `properties: {}` in YAML ( under `microbenchmark/properties`) implies user has made their own customworkload class overriding the create(), loadOnce and executeOnce() of YBMicrobenchmark abstract class.
2.  If you are using `execute` or `executeOnce` in YAML set the flag `setAutoCommit` to `false`.
### Utility Functions:-

Utility functions are present inside the folder :-
`src/main/java/com/oltpbenchmark/benchmarks/featurebench/utils`

| Name                       | Description                                                                                                                          | Parameters                          | Parameter Type             |
|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------|----------------------------|
| CurrentTime                | Returns SQL TIMESTAMP value.                                                                                                         | None                                | None                       |
| HashedPrimaryStringGen     | Return unique md5 hash which is generated based on given number and length of hash                                                   | startNumber,length                  | Integer,Integer            |
| HashedRandomString         | Return random md5 hash which is generated based on given range and length of hash	                                                   | minimum,maximum,length              | Integer,Integer,Integer    |
| OneNumberFromArray         | Returns a number randomly from a predefined array of numbers passed by user.                                                         | listOfIntegers                      | List of Integers           |
| OneStringFromArray         | Returns a string randomly from a predefined array of strings passed by user.                                                         | list of str                         | List of Strings            |
| OneUUIDFromArray           | Returns a UUID randomly from a predefined array of UUIDs passed by user.                                                             | list of str                         | List of Strings            |
| PrimaryDateGen             | Generates unique date based on given total no. of unique dates required                                                              | totalUniqueDates                    | Integer                    |
| PrimaryFloatGen            | Unique Float Generator between given range with decimal points.                                                                      | lowerRange,upperRange,decimalPoint  | Integer,Integer,Integer    |
| PrimaryIntGen              | Integer Primary key generator between a range.(sequential incremental keys generated).                                               | lowerRange,upperRange               | Integer,Integer            |
| RandomUniqueIntGen         | Generated random integer from lower range to upper range and all of them are unique.                                                 | lowerRange,upperRange               | Integer,Integer            |
| PrimaryStringGen           | String Integer Primary key generator starting from a numeric no.(sequential incremental numeric strings generated).                  | startNumber,desiredLength           | Integer,Integer            |
| PrimaryIntRandomForExecutePhase | Generates Random unique int based on given range                                                                                     | lowerRange,upperRange               | Integer,Integer            |
| RandomAString              | Returns a random alphabetic string with length in range [minimumlength,maximumlength].                                               | minimumLength,maximumLength         | Integer,Integer            |
| RandomBoolean              | Returns a random boolean value.                                                                                                      | None                                | None                       |
| RandomBytea                | Returns returns a random hexadecimal string with length in range [minimumlength,maximumlength]                                       | minimumLength,maximumLength         | Integer,Integer            |
| RandomDate                 | Returns a random date string in range [yearlowerBound,yearupperBound]                                                                | yearlowerBound,yearupperBound       | Integer,Integer            |
| RandomInt                  | Returns a random int value between minimum and maximum (inclusive).                                                                  | minimum,maximum                     | Integer,Integer            |
| CyclicSeqIntGen            | Returns values between lower bound and upper bound including these bounds and repeats the sequence in cyclic way.                    | lowerRange, upperRange              | Integer, Integer           |
| RandomFloat                | Returns a random Float value between given range and specified decimal point                                                         | minimum,maximum,decimalPoint        | Integer,Integer,Integer    |
| RandomJson                 | Returns a random json with pre-defined number of fields, size of values, level of nestedness(optional - nestedness=1 for now)        | fields,valueLength,nestedness       | Integer,Integer,Integer    |
| RandomLong                 | Returns a random long value between minimum and maximum (inclusive)                                                                  | minimum, maximum                    | Long,Long                  |
| RandomNoWithDecimalPoints  | Returns a random double in the range[minimum,maximum] with fixed decimal places.                                                     | lowerBound,upperBound,decimalPoints | Integer,Integer,Integer    |
| RandomNstring              | Returns a random numeric string with length in range [minimumLength,maximumLength].                                                  | minimumLength,maximumLength         | Integer,Integer            |
| RandomNumber               | Returns a random number in range [minimum,maximum].                                                                                  | minimum,maximum                     | Integer,Integer            |
| RandomStringAlphabets      | Returns a random alphabetic string of passed length.                                                                                 | desiredLength                       | Integer                    |
| RandomStringNumeric        | Returns a random numeric string of passed length.                                                                                    | desiredLength                       | Integer                    |
| RandomUUID                 | Returns a random  UUID                                                                                                               | None                                | None                       |
| RowRandomBoundedInt        | Returns a random int in the range[lowValue,highValue]                                                                                | lowValue,highValue                  | Integer,Integer            |
| RowRandomBoundedLong       | Returns a random long in the range[lowValue,highValue]                                                                               | lowValue,highValue                  | Long,Long                  |
| RandomDateBtwYears         | Returns a random date in the range[yearLowerBound,yearUpperBound]                                                                    | yearLowerBound,yearUpperBound       | Integer,Integer            |
| RandomPKString             | Returns a random Primary key of String type in range [startNumber, endNumber] of desired length.(extra characters appended with 'a') | startNumber,endNumber,desiredLength | Integer,Integer,Integer    |
| RandomTextArrayGen         | Returns an array of string with length lies between minimum and maximum .                                                            | arraySize, minLength,maxLength      | Integer,Integer,Integer    |
| RandomTimestamp            | Returns a random Timestamp which are determinsitic based on total no. of unique timestamps required                                  | totalTimestamps                     | Integer                    |
| RandomTimestampWithoutTimeZone | Returns a random Timestamp without timezone which are determinsitic based on total no. of unique timestamps required                 | totalTimestamps                     | Integer                    |
| RandomTimestampWithTimeZone | Returns a random Timestamp with timezone which are determinsitic based on total no. of unique timestamps required                    | totalTimestamps                     | Integer                    |
| RandomTimestampWithTimezoneBetweenDates | Returns a random Timestamp with timezone between the dates provided.                                                                 | start date, end date                | String(date), string(date) |
| RandomTimestampWithTimezoneBtwMonths | Returns a random Timestamp with timezone between months of the same year                                                             | year, start month, end month        | Integer, Integer, Integer  |


### Results:-

The results for each YAML config run can be found inside the `results` folder.

Serial runs inside a YAML have separate sub-folders inside `results` folder with names same as either run name(if provided) or timestamp of run.





















