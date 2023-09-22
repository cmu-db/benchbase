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

> Since the templated benchmark is meant to flexibly support a myriad of different database schemas, it doesn't *currently* support the `init` and `load` phases.
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
