# Templated Benchmarks

This class is used to execute templated benchmarks, i.e., benchmark queries that have parameters that the user wants to set dynamically.
A templated benchmark config has the following structure:

```xml
<templates>
    <template name="$QueryTemplateName">
        <query><![CDATA[$SQLQuery]]></query>
        <types>
            <type>$ParameterType1</type>
            <type>$ParameterType2</type>
        </types>
        <values>
            <value>$ParameterValueA1</value>
            <value>$ParameterValueA2</value>
            <!-- ... -->
        </values>
        <values>
            <value>$ParameterValueB1</value>
            <value>$ParameterValueB2</value>
            <!-- ... -->
        </values>
    </template>
    <!-- ... -->
<templates>
```

where `$ParameterType` is the `java.sql.Types` value (i.e., Integer, Boolean, etc.) and each value tag within `values` contains the values for one instantiation of the parameters set in `$SQLQuery`.
The SQL query string is read as a `PreparedStatement`, i.e., parameters are defined in the string via a `?` placeholder.

An example for a templated benchmark can be found in [`data/templated/example.xml`](../../../../../../../data/templated/example.xml).
The file path for the XML template has to be defined in the workload configuration using the `templates_file` tag.

An example configuration can be found in [`config/sqlserver/sample_template_config.xml`](../../../../../../../config/sqlserver/sample_templated_config.xml).

> Since the templated benchmark is meant to flexibly support a myriad of different database schemas, it doesn't _currently_ support the `init` and `load` phases.

<!-- TODO: Add support for init/load phases? -->

The example can be executed if a loaded TPC-C instance is used as JDBC endpoint.

For instance:

```sh
java -jar benchbase.jar -b tpcc -c config/sqlserver/sample_tpcc_config.xml --create=true --load=true --execute=false
```

Templated benchmarks are instantiated using `templated` as benchmark class when running BenchBase via the command line.

For instance:

```sh
java -jar benchbase.jar -b templated -c config/sqlserver/sample_templated_config.xml --create=false --load=false --execute=true --json-histograms results/histograms.json
```

> For additional examples, please refer to the build pipeline definition in the [`maven.yml`](../../../../../../../.github/workflows/maven.yml#L423) Github Actions workflow file.

## Value Distributions

In order to support more variety in templated queries, it is possible to use a whole distribution of values instead of a single static value in a templated query

Each time the query is run, a different value will substitute the `?`.

```xml
<templates>
    <template name="MyTemplate">
        <query><![CDATA[SELECT * FROM MyTable WHERE id = ?]]></query>
        <types>
            <type>INTEGER</type>
        </types>
        <values>
            <value dist="uniform" min="0" max="1000" seed="1"/>
        </values>
         <values>
            <value dist="zipf" max="1000" seed="1"/>
        </values>
    </template>
    <!-- ... -->
<templates>
```

The distributions are dependent on the type of the value. Currently, the following type-distributions pairs are supported:
| Type             | uniform | binomial | zipfian | scrambled |
| ---------------- | :-----: | :------: | :-----: | :---------------: |
| INTEGER          |    X    |    X     |    X    |         X         |
| FLOAT / REAL     |    X    |    X     |    -    |         -         |
| BIGINT           |    X    |    X     |    X    |         X         |
| VARCHAR          |    X    |    -     |    -    |         -         |
| TIMESTAMP        |    X    |    X     |    X    |         X         |
| DATE             |    X    |    X     |    X    |         X         |
| TIME             |    X    |    X     |    X    |         X         |
The following properties can be set on the value:

-   `dist`: The distribution of the values
-   `min`: The minimum value the generator can produce. Defaults to 0
-   `max`: The maximum value the generator can produce. Defaults to 1
-   `seed`: A seed for the generator to ensure consistency

*NOTE:* For `Timestamp`, `Date`, and `Time` types, the min and max values must be converted to UNIX time (milliseconds).
The value is interpreted as the milliseconds since January 1, 1970, 00:00:00 GMT/UTC.
```xml
<value 
    dist="uniform" 
    min="1674477220000" // Mon Jan 23 2023 12:33:40 
    max="1706013220000" // Tue Jan 23 2024 12:33:40 
    seed="0"
/>
```
To get the current UNIX time in milliseconds, use the following bash command:
```bash
date +%s%3N
```
or use a web services like [www.currentmillis.com](www.currentmillis.com)


