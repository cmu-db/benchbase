"""Module that handles all things related to configuration parsing
"""

import sys
import xml.etree.ElementTree as ET

from configuration.configurations import (
    DPConfig,
    DPColumnConfig,
    SensitiveConfig,
    SensitiveEntry,
    ContinuousConfig,
    ContinuousEntry,
)


class XMLParser:
    """A class to represent a specific XML parser for BenchBase configuration files

    Attributes
    ----------
    table : ET.Element
          XML element with the <table> - tag
    """

    def __init__(self, table: ET.Element):
        self.table = table

    def get_config(self):
        """Function that extracts the different types of configuration classes from the XML tree

        Returns:
            (DPConfig,ContinuousConfig,SensitiveConfig): The three configuration classes
        """

        anon_config = None
        cont_config = None
        sens_config = None
        continuous_entries = []
        sensitive_entries = []

        table_name = self.table.get("name")

        # Exit the program if not enough basic information (name of a table) is available
        if table_name is None:
            sys.exit(
                "There was no name provided for the table that should be anonymized. Program is exiting now!"
            )

        print(f"Parsing config for table: {table_name}")

        dp_info = self.table.find("differential_privacy")

        if dp_info is not None:

            cat = self.__get_column_type("ignore", dp_info)
            ordi = self.__get_column_type("ordinal", dp_info)
            ignore = self.__get_column_type("continuous", dp_info)

            eps = dp_info.get("epsilon", "1.0")
            pre_eps = dp_info.get("pre_epsilon", "0.5")
            alg = dp_info.get("algorithm", "mst")

            # Specific handling for continuous columns to incorporate a separate config
            cont = []
            if dp_info.find("continuous") is not None:
                for column in dp_info.find("continuous").findall("column"):
                    cont.append(column.get("name"))

                    if column.get("bins") or column.get("lower") or column.get("upper"):
                        continuous_entries.append(
                            ContinuousEntry(
                                column.get("name"),
                                column.get("bins", "10"),
                                column.get("lower"),
                                column.get("upper"),
                            )
                        )

            column_classes = DPColumnConfig(ignore,cat,cont,ordi)

            anon_config = DPConfig(
                table_name, eps, pre_eps, alg, column_classes
            )

            if len(cont) > 0:
                cont_config = ContinuousConfig(continuous_entries)

        sens = self.table.find("value_faking")
        if sens is not None:
            for column in sens.findall("column"):
                sensitive_entries.append(
                    SensitiveEntry(
                        column.get("name"),
                        column.get("method"),
                        column.get("locales"),
                        column.get("seed", "0"),
                    )
                )
            sens_config = SensitiveConfig(sensitive_entries)

        return anon_config, sens_config, cont_config

    def __get_column_type(self, keyword: str, subtree: ET.Element):
        """Helper method to extract column types

        Args:
            keyword (str): Column type keyword
            subtree (ET.Element): The subtree in which to search

        Returns:
            list[str]: A list of column names with the specific type
        """
        tmp = []
        if subtree.find(keyword) is not None:
            for column in subtree.find(keyword).findall("column"):
                tmp.append(column.get("name"))
        return tmp
