# Anonymization

<span style="color:orange"> ANONYMIZATION IS A WORK IN PROGRESS AND DOES CURRENTLY NOT ACTUALLY ANONYMIZE THE DATA. THIS FEATURE WILL BE ADDED LATER </span>

The anonymization module allows to apply privacy mechanisms such as differential privacy or column faking to the data. 
The system will pull data from the JDBC connection, anonymize the data and push it back to the DBMS by creating a new table.

## Config

### Default Parameters

The default parameters must be added inside of the `<table>`-tag. (See below)

| Name        | Default | Possible Value                      | Description                                                                                  |
| ----------- | ------- | ----------------------------------- | -------------------------------------------------------------------------------------------- |
| name        | -       | Any string                          | The name of the table                                                                        |
| epsilon     | 1.0     | Any value > 0                       | The privacy budget. Higher values will decrease the privacy guarantee                        |
| pre_epsilon | 0.5     | Any value between 0 and epsilon     | The privacy budget spent on preprocessing of the data. Only necessary for continuous columns |
| algorithm   | mst     | One of: `mst,aim,dpctgan,patectgan` | The differential privacy mechanism applied to the data                                       |

```xml
<anonymization>
    <table
        name="tablename"
        epsilon="1.5"
        pre_epsilon="0.75"
        algorithm="patectgan"
    />
</anonymization>
```

### Column Information

The columns of a table can be split into four categories:

1. `drop` - Columns that are omitted from the DP-mechanism
2. `categorical` - Columns that contain categorical data
3. `continuous` - Columns of data on a continuous domain. Only numerical columns
4. `ordinal` - Columns of ordinal meaning. Typically numerical columns with low amount of distinct values

The names of the columns must be given as text. Multiple column names must be separated by comma without additional whitespace.

The anonymization process will automatically try to infer the category best suitable category of each column. Sometimes this leads to undesired results or is unable to process the column because of other factors. To counter this, each column can be defined by hand with the suitable tags.

---

**Drop the ID-column and anonymize automatically:**

```xml
<anonymization>
    <table name="item">
        <drop>i_id</drop>
    </table>
</anonymization>
```

The ID-column will be removed from the anonymization process and be added back to the data in its original form! This is useful for primary key columns or columns that contain only NULL values.

---

**Define the type of each column individually:**

```xml
<anonymization>
    <table name="item">
        <categorical>i_name,i_data,i_im_id</categorical>
        <continuous>i_price</continuous>
        <ordinal>i_id</ordinal>
    </table>
</anonymization>
```

<span style="color:red"> **Disclaimer**: As soon as the column types are defined by hand, all columns must be defined. It is not possible to only specify some of the categorical columns and let the algorithm do the rest!</span>

### Sensitive Values

The anonymization module supports value faking to handle sensitive values. A `<sensitive>`-tag must be created to signify that column faking should be applied. Column faking can be defined individually on each column. To do so, parameters can be added inside of the `<column>`-tag. (See below)

| Name    | Default | Possible Value                                   | Description                                                                                    |
| ------- | ------- | ------------------------------------------------ | ---------------------------------------------------------------------------------------------- |
| name    | -       | Any string                                       | The name of the column                                                                         |
| method  | pystr   | Any method provided by the python faker library  | The faking method of the library. If the method is not found, a random string will be inserted |
| locales | -       | A string of supported locales separated by comma | The locale to produce localized values like chinese street names or english first names        |
| seed    | 0       | Any integer value                                | The privacy budget spent on preprocessing of the data. Only necessary for continuous columns   |

```xml
<anonymization>
    <table name="item" algorithm="aim">
        <sensitive>
            <column
                name="i_name"
                method="name"
                locales="en_US"
                seed="0">
            </column>
            <column name="i_data" method="pystr" />
        </sensitive>
    </table>
</anonymization>
```

## Examples

### Basic

The most basic config will need only the name of the table and apply default values for the anonymization.

```xml
<anonymization>
    <table name="item"/>
</anonymization>
```

It is possible to specify multiple tables for the anonymization. Each table will be anonymized on its own

```xml
<anonymization>
    <table name="item"/>
    <table name="history"/>
    <table name="customer"/>
</anonymization>
```

The parameters of the process can be tuned manually

```xml
<anonymization>
    <table name="item" epsilon="1.5" pre_epsilon="0.5" />
</anonymization>
```

### Full config

It is possible to add information about the columns to enable fine-tuning.

```xml
<anonymization>
    <table
        name="item"
        epsilon="1.0"
        pre_epsilon="0.0"
        algorithm="aim"
    >
        <drop>i_id</drop>
        <categorical>i_name,i_data,i_im_id</categorical>
        <continuous>i_price</continuous>
    </table>
</anonymization>
```

### Sensitive value handling

Sensitive columns can be anonymized further by replcing the values with fake values

```xml
<anonymization>
    <table
        name="item"
        epsilon="1.0"
        pre_epsilon="0.0"
        algorithm="aim">
        <ordinal>i_id</ordinal>
        <categorical>i_name,i_data,i_im_id</categorical>
        <continuous>i_price</continuous>
        <sensitive>
            <column
                name="i_name"
                method="name"
                locales="en_US"
                seed="0"
            />
            <column
                name="i_data"
                method="pystr"
                seed="0"
            />
        </sensitive>
    </table>
</anonymization>
```
