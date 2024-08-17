"""Module that handles the full Anonymization pipeline
"""

import sys
import xml.etree.ElementTree as ET
import pandas as pd
from modules.jdbc_handler import JDBCHandler
from configuration.config_parser import XMLParser
from configuration.configurations import DPConfig, SensitiveConfig, ContinuousConfig
from modules.dp_anonymizer import DifferentialPrivacyAnonymizer
from modules.sensitive_anonymizer import SensitiveAnonymizer


def anonymize(
    dataset: pd.DataFrame,
    anon_config: DPConfig,
    cont_config: ContinuousConfig,
    sens_config: SensitiveConfig,
):
    
    dp_data = dataset
    if anon_config:
        dp_anonymizer = DifferentialPrivacyAnonymizer(dataset, anon_config, cont_config)
        dp_data = dp_anonymizer.run_anonymization()

    if sens_config:
        sens_anonymizer = SensitiveAnonymizer(dp_data,sens_config)
        dp_data = sens_anonymizer.run_anonymization()

    return dp_data


def anonymize_db(
    jdbc_handler: JDBCHandler,
    anon_config: DPConfig,
    sens_config: SensitiveConfig,
    cont_config: ContinuousConfig,
):
    
    jdbc_handler.start_jvm()

    conn = jdbc_handler.get_connection()

    table = anon_config.table_name
    dataset, timestamps = jdbc_handler.data_from_table(conn, table)

    dataset_anon = anonymize(
        dataset, anon_config, cont_config, sens_config
    )

    # Create empty table
    anon_table_name = jdbc_handler.create_anonymized_table(conn, table)

    # Populate new table
    jdbc_handler.populate_anonymized_table(
        conn, dataset_anon, anon_table_name, timestamps
    )

    conn.close()
    
    return



def main():
    """Entry method"""

    if len(sys.argv) >= 2:
        xml_config_path = sys.argv[1]

    else:
        print("Not enough arguments provided: <configPath>")
        return

    tree = ET.parse(xml_config_path)

    parameters = tree.getroot()

    jdbc_handler = JDBCHandler(
        parameters.find("driver").text,
        parameters.find("url").text,
        parameters.find("username").text,
        parameters.find("password").text,
    )

    # Loop over all specified tables and anonymize them one-by-one
    for table in parameters.find("anonymization").findall("table"):
        config_parser = XMLParser(table)
        anon_config, sens_config, cont_config = config_parser.get_config()

        anonymize_db(jdbc_handler, anon_config, sens_config, cont_config)


if __name__ == "__main__":
    main()
