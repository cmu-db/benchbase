This class is used to execute templated benchmarks, i.e., benchmarks that have parameters that the user wants to set dynamically. A templated benchmarkhas the structure
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
            ...
        </values>
        <values>     
            <value>$ParameterValueB1</value>
            <value>$ParameterValueB2</value>
            ...
        </values> 
    </template> ...
<templates>
where $ParameterType is the `java.sql.Types` value (i.e., Integer, Boolean, etc.) and each value tag within 'values' contains the values for one instantiation of the parameters set in `$SQLQuery`. The SQL query string is read as a `PreparedStatement`, i.e., parameters are defined in the string via a '?' placeholder. An example for a templated benchmark can be found in data/templated/example.xml. The file path for the XML template has to be defined in the workload configuration using the 'templates_file' tag. An example configuration can be found inconfig/sqlserver/sample_template_config.xml. The example can be executed if a loaded TPC-C instance is used as JDBC endpoint. Templated benchmarks are instantiated using 'templated' as benchmark class when running BenchBase via the command line.