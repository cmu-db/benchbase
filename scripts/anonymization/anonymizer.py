"""Module that handles the full Anonymization pipeline
"""

import sys
import xml.etree.ElementTree as ET


def rewriteFakeQueries(fakeValues, path):
    """Method that rewrites existing query templates based on sensitive value replacements
    """
    # TODO


def fakeColumn(dataset, col, locales, method, seed=0):
    """Method that generates fake values for columns that are considered sensitive
    """
    # TODO


def getTimestampColumns(dbTypeList):
    """A helper function that returns a list of indexes of timestamp-type columns
    """
    # TODO


def dfFromTable(curs, table):
    """Helper function that creates a pandas DataFrame from a jaydebe connection
    """
    # TODO


def populateAnonFromDF(curs, df, table, timestampIndexes):
    """Helper function to fill a DB table from a DataFrame
    """
    # TODO


def getDroppableInfo(dropCols, dataset):
    """Helper function that saves droppable columns from anonymization
    """
    # TODO


def getTransformer(
    dataset, algorithm, categorical, continuous, ordinal, continuousConfig
):
    """Function that creates a custom transformer for the dp-mechanism
    """

    # TODO


def getConstraints(objects):
    """A helper method that builds constraints that are used by a TableTransformer
    """
    # TODO


def anonymize(
    dataset: str,
    anonConfig: dict,
    sensConfig: list,
    contConfig: list,
    templatesPath: str,
):
    """Function that handles the data anonymization step
    """
    # TODO


def anonymizeDB(
    jdbcConfig: dict,
    anonConfig: dict,
    sensConfig: list,
    contConfig: list,
    templatesPath: str,
):
    """Function that handles the necessary steps for anonymization.
    Includes starting the JVM, Anonymizing and pushing data to the DB
    """
    # TODO


def listFromElement(element):
    """Helper function creating an array of values from a XML Element
    """
    if element and element.text:
        return list(element.text.split(","))
    else:
        return []


def configFromXML(table):
    """Method that constructs a configuration dictionary from XML for a specified table

    Args:
        table (Element): The XML element that describes the table

    Returns:
        (dict,dict,dict): The configurations for the anonymization, sensitive and continuous value handling
    """

    anonConfig = {}
    sensConfig = []
    contConfig = []

    # Necessary information
    tableName = table.get("name")

    if tableName == None:
        sys.exit(
            "There was no name provided for the table that should be anonymized. Program is exiting now!"
        )

    anonConfig["table"] = tableName
    print(f"Parsing config for table: {tableName}")

    anonConfig["eps"] = table.get("epsilon", 1.0)
    anonConfig["preEps"] = table.get("pre_epsilon", 0.5)
    anonConfig["alg"] = table.get("algorithm", "mst")

    # Additional information

    anonConfig["hide"] = listFromElement(table.find("drop"))

    anonConfig["cat"] = listFromElement(table.find("categorical"))

    anonConfig["cont"] = listFromElement(table.find("continuous"))

    anonConfig["ord"] = listFromElement(table.find("ordinal"))

    cont = table.find("continuousConfig")

    if cont:
        for col in cont.findall("column"):
            contConfig.append(
                {
                    "name": col.get("name"),
                    "bins": col.get("bins"),
                    "lower": col.get("lower"),
                    "upper": col.get("upper"),
                }
            )

    sens = table.find("sensitive")
    if sens:
        for sensCol in sens.findall("column"):
            sensConfig.append(
                {
                    "name": sensCol.get("name"),
                    "method": sensCol.get("method"),
                    "mode": sensCol.get("mode"),
                    "locales": sensCol.get("locales"),
                    "seed": sensCol.get("seed", 0),
                }
            )

    return anonConfig, sensConfig, contConfig


def main():
    """Entry method"""

    # No templates provided
    if len(sys.argv) == 2:
        confPath = sys.argv[1]
        templatesPath = ""

    elif len(sys.argv) == 3:
        confPath = sys.argv[1]
        templatesPath = sys.argv[2]

    else:
        print("Not enough arguments provided: <configPath> <templatesPath (optional)>")
        return

    tree = ET.parse(confPath)

    parameters = tree.getroot()
    jdbcConfig = {
        "driver": parameters.find("driver").text,
        "url": parameters.find("url").text,
        "username": parameters.find("username").text,
        "password": parameters.find("password").text,
    }

    # Loop over all specified tables and anonymize them one-by-one
    for table in parameters.find("anonymization").findall("table"):
        anonConfig, sensConfig, contConfig = configFromXML(table)
        anonymizeDB(jdbcConfig, anonConfig, sensConfig, contConfig, templatesPath)


if __name__ == "__main__":
    main()
