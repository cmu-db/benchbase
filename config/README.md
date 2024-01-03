# Config

The config files are duplicated in a seperate folder for each of the supported database systems.

For example, the postgres/sibench config file has the following pieces:

## Connection Details

The connection details allow for the configuration of the connection to the database via JDBC.

```xml
    <!-- Connection details -->
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:5432/benchbase?sslmode=disable&amp;ApplicationName=sibench&amp;reWriteBatchedInserts=true</url>
    <username>admin</username>
    <password>password</password>
    <isolation>TRANSACTION_SERIALIZABLE</isolation>
    <batchsize>128</batchsize>
```

## Workload

-   `scalefactor` allows to upscale the workload by the specified factor
-   `time` allows to modify the runtime of the workload. It is given in **seconds**
-   `weights` define the weight of each [transaction Type](#procedures). The weights are defined as **percentages**

```xml

    <scalefactor>1</scalefactor>

    <!-- The workload -->
    <terminals>1</terminals>
    <works>
        <work>
            <time>60</time>
            <rate>unlimited</rate>
            <weights>50,50</weights>
        </work>
    </works>
```

## Procedures

Each workload specifies certain procedures which can be removed by deleting the `transactionType` from the config. By default, all transaction types of the workload are listed in an unmodified config file.

```xml
    <!-- SIBENCH Procedures declaration -->
    <transactiontypes>
        <transactiontype>
            <name>MinRecord</name>
        </transactiontype>
        <transactiontype>
            <name>UpdateRecord</name>
        </transactiontype>
    </transactiontypes>
</parameters>

```

## Workload Specific Configs

### Templated

The templated workload demands a path to a file that holds the templated queries.

```xml
<query_templates_file>data/templated/example.xml</query_templates_file>
```
