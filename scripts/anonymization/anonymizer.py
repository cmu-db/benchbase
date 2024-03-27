"""Module that handles the full Anonymization pipeline
"""

import sys
import xml.etree.ElementTree as ET

from configuration.configurations import DPConfig, SensitiveConfig, ContinuousConfig
from modules.jdbc_handler import JDBCHandler
from scripts.anonymization.configuration.config_parser import XML_parser
#from modules.dp_anonymizer import DifferentialPrivacyAnonymizer
#from modules.sensitive_anonymizer import SensitiveAnonymizer


"""Module that handles the full Anonymization pipeline
"""

import sys
import pandas as pd

"""
def anonymize(
    dataset: pd.DataFrame,
    anon_config: DPConfig,
    cont_config: ContinuousConfig,
    sens_config: SensitiveConfig,
    templates_path: str,
):
    dp_data = dataset
    if anon_config:
        dp_anonymizer = DifferentialPrivacyAnonymizer(dataset, anon_config, cont_config)
        dp_data = dp_anonymizer.run_anonymization()

    if sens_config:
        sens_anonymizer = SensitiveAnonymizer(dp_data,sens_config,templates_path)
        dp_data = sens_anonymizer.run_anonymization()

    return dp_data


def anonymizeDB(
    jdbc_handler: JDBCHandler,
    anon_config: DPConfig,
    sensConfig: SensitiveConfig,
    contConfig: ContinuousConfig,
    templates_path: str,
):
    jdbc_handler.start_JVM()

    conn = jdbc_handler.get_connection()

    table = anon_config.table_name
    dataset, timestamps = jdbc_handler.data_from_table(conn, table)

    datasetAnon = anonymize(
        dataset, anon_config, contConfig, sensConfig, templates_path
    )

    ## TODO: Throw in Sensitive Anonmization

    # Create empty table
    anon_table_name = jdbc_handler.create_anonymized_table(conn, table)

    # Populate new table
    jdbc_handler.populate_anonymized_table(
        conn, datasetAnon, anon_table_name, timestamps
    )

    conn.close()
"""


def main():
    """Entry method"""

    # No templates provided
    if len(sys.argv) == 2:
        confPath = sys.argv[1]
        templates_path = ""

    elif len(sys.argv) == 3:
        confPath = sys.argv[1]
        templates_path = sys.argv[2]

    else:
        print("Not enough arguments provided: <configPath> <templates_path (optional)>")
        return

    tree = ET.parse(confPath)

    parameters = tree.getroot()

    jdbc_handler = JDBCHandler(
        parameters.find("driver").text,
        parameters.find("url").text,
        parameters.find("username").text,
        parameters.find("password").text,
    )

    # Loop over all specified tables and anonymize them one-by-one
    for table in parameters.find("anonymization").findall("table"):
        config_parser = XML_parser(table)
        anon_config, sens_config, cont_config = config_parser.get_config()

        #anonymizeDB(jdbc_handler, anon_config, sens_config, cont_config, templates_path)


if __name__ == "__main__":
    main()
