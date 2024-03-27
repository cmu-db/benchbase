import sys
import xml.etree.ElementTree as ET

from .configurations import (
    DPConfig,
    SensitiveConfig,
    SensitiveEntry,
    ContinuousConfig,
    ContinuousEntry,
)


class XML_parser:

    def __init__(self, table: ET.Element):
        self.table = table

    def get_config(self):

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
            cat = []
            cont = []
            ord = []
            ignore = []

            eps = dp_info.get("epsilon", "1.0")
            pre_eps = dp_info.get("pre_epsilon", "0.5")
            alg = dp_info.get("algorithm", "mst")

            if dp_info.find("ignore"):
                for column in dp_info.find("ignore").findall("column"):
                    ignore.append(column.get("name"))

            if dp_info.find("categorical"):
                for column in dp_info.find("categorical").findall("column"):
                    cat.append(column.get("name"))

            if dp_info.find("ordinal"):
                for column in dp_info.find("ordinal").findall("column"):
                    ord.append(column.get("name"))

            if dp_info.find("continuous"):
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

            anon_config = DPConfig(
                table_name, eps, pre_eps, alg, ignore, cat, cont, ord
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
