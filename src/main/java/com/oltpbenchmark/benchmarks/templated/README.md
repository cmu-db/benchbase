This class is used to execute templated benchmarks, i.e., benchmarks that have parameters that the user wants to set dynamically. A templated benchmarkhas the structure
<query_templates> 
    <query_template>   
        <name>$QueryTemplateName</name>   
        <query><![CDATA[$SQLQuery]]></query>   
        <parameter_types>
            <parameter_type>$ParameterType1</parameter_type>
            <parameter_type>$ParameterType2</parameter_type>
        </parameter_types>   
        <parameter_values>     
            <parameter_value>$ParameterValueA1</parameter_value>
            <parameter_value>$ParameterValueA2</parameter_value>
            ...
        </parameter_values>
        <parameter_values>     
            <parameter_value>$ParameterValueB1</parameter_value>
            <parameter_value>$ParameterValueB2</parameter_value>
            ...
        </parameter_values> 
    </query_template> ...
<query_templates>
where $ParameterType is the integer java.sql.Types value (i.e., 4 forinteger, 16 for boolean etc.) and each value tag within 'parameter_values' contains the values for one instantiation of the parameters set in $SQLQuery. The SQL query string is read as a PreparedStatement, i.e., parameters are defined in the string via a '?' placeholder. An example for a templated benchmark can be found in data/templated/example.xml. The file path for the XML template has to be defined in the workload configuration using the'query_templates_file' tag. An example configuration can be found inconfig/sqlserver/sample_template_config.xml. The example can be executed if a loaded TPC-C instance is used as JDBC endpoint.Templated benchmarks are instantiated using 'templated' as benchmark classwhen running BenchBase via the command line.