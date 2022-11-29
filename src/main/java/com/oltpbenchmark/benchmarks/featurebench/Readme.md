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

| Name                      | Description                                                                                                                           | Parameters                              | Parameter Type           |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|--------------------------|
| ArrayOfDates              | Returns an array of random dates. User has to send the array size.                                                                    | arraySize                               | Integer                  |
| ConstantValue             | Returns the value(constant) passed to it.                                                                                             | constantVal                             | List of Objects          |
| CurrentTime               | Returns SQL TIMESTAMP value.                                                                                                          | None                                    | None                     |
| CurrentTimeString         | Returns the Current Time in yyyy-MM-dd_HH-mm-ss format.                                                                               | None                                    | None                     |
| CurrentTimeString14       | Returns the CurrentTime in yyyyMMddHHmmss format.                                                                                     | None                                    | None                     |
| GenerateRandomString      | Returns a random string from an array of random strings generated at runtime(length of random string and array passed as parameters). | desiredLength,sizeOfStringArray         | Integer ,Integer         |
| OneNumberFromArray        | Returns a number randomly from a predefined array of numbers passed by user.                                                          | listOfIntegers                          | List of Integers         |
| OneStringFromArray        | Returns a string randomly from a predefined array of strings passed by user.                                                          | str                                     | List of Strings          |
| PrimaryIntGen             | Integer Primary key generator between a range.(sequential incremental keys generated).                                                | upperRange,lowerRange                   | Integer,Integer          |
| PrimaryStringGen          | String Integer Primary key generator starting from a numeric no.(sequential incremental numeric strings generated).                   | startNumber,desiredLength               | Integer,Integer          |
| RandomAString             | Returns a random alphabetic string with length in range [minimumlength,maximumlength].                                                | minimumLength,maximumLength             | Integer,Integer          |
| RandomBoolean             | Returns a random boolean value.                                                                                                       | None                                    | None                     |
| RandomDate                | Returns a random date string in range [yearlowerBound,yearupperBound]                                                                 | yearlowerBound,yearupperBound           | Integer,Integer          |
| RandomExpoFloat           | Returns a random exponential distribution float value with average equal to center.                                                   | center,deviation                        | Double,Double            |
| RandomExpoInt             | Returns a random exponential distribution int value with average equal to center.                                                     | center,deviation                        | Integer,Integer          |
| RandomExpoLong            | Returns a random exponential distribution long value with average equal to center.                                                    | center,deviation                        | Long,Long                |
| RandomInt                 | Returns a random int value between minimum and maximum (inclusive).                                                                   | minimum,maximum                         | Integer,Integer          |
| RandomJson                | Returns a random json with pre-defined number of fields, level of nestedness, size of keys, size of values (nestedness=1 for now)     | fields,nestedness,valueType,valueLength | Integer,1,Object,Integer |
| RandomLong                | Returns a random long value between minimum and maximum (inclusive)                                                                   | minimum, maximum                        | Long,Long                |
| RandomNormalFloat         | Returns a random normal distribution float value with average equal to center                                                         | center,deviation                        | Double,Double            |
| RandomNormalInt           | Returns a random normal distribution int value with average equal to center                                                           | center,deviation                        | Integer,Integer          |
| RandomNormalLong          | Returns a random normal distribution long value with average equal to center                                                          | center,deviation                        | Long,Long                |
| RandomNoWithDecimalPoints | Returns a random double in the range[minimum,maximum] with fixed decimal places.                                                      | lowerBound,upperBound,decimalPoints     | Integer,Integer,Integer  |
| RandomNstring             | Returns a random numeric string with length in range [minimumLength,maximumLength].                                                   | minimumLength,maximumLength             | Integer,Integer          |
| RandomNumber              | Returns a random number in range [minimum,maximum].                                                                                   | minimum,maximum                         | Integer,Integer          |
| RandomStringAlphabets     | Returns a random alphabetic string of passed length.                                                                                  | desiredLength                           | Integer                  |
| RandomStringNumeric       | Returns a random numeric string of passed length.                                                                                     | desiredLength                           | Integer                  |
| RandomUUID                | Returns a random String UUID                                                                                                          | None                                    | None                     |
| RowRandomBoundedInt       | Returns a random int in the range[lowValue,highValue]                                                                                 | lowValue,highValue                      | Integer,Integer          |
| RowRandomBoundedLong      | Returns a random long in the range[lowValue,highValue]                                                                                | lowValue,highValue                      | Long,Long                |
| RandomDateBtwYears        | Returns a random date in the range[yearLowerBound,yearUpperBound]                                                                     | yearLowerBound,yearUpperBound           | Integer,Integer          |
| RandomNumberDefault       | Returns a random number in range [Integer.MIN_VALUE,Integer.MAX_VALUE]                                                                | None                                    | None                     |
| RandomPKString            | Returns a random Primary key of String type in range [startNumber, endNumber] of desired length.(extra characters appended with 'a')  | startNumber,endNumber,desiredLength     | Integer,Integer,Integer  |

### Results:-

The results for each YAML config run can be found inside the `results` folder.

Serial runs inside a YAML have separate sub-folders inside `results` folder with names same as either run name(if provided) or timestamp of run.





















