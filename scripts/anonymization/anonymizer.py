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
    anon_config: dict,
    sens_config: list,
    cont_config: list,
    templatesPath: str,
):
    """Function that handles the data anonymization step
    """
    # TODO


def anonymizeDB(
    jdbcConfig: dict,
    anon_config: dict,
    sens_config: list,
    cont_config: list,
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

    anon_config = {}
    sens_config = []
    cont_config = []

    tableName = table.get("name")

    #Exit the program if not enough basic information (name of a table) is available
    if tableName == None:
        sys.exit(
            "There was no name provided for the table that should be anonymized. Program is exiting now!"
        )

    anon_config["table"] = tableName
    print(f"Parsing config for table: {tableName}")

    dp_info = table.find("differential_privacy")

    if(dp_info):

        cat = []
        cont = []
        ord = []
        ignore = []
        
        anon_config["eps"] = dp_info.get("epsilon", 1.0)
        anon_config["preEps"] = dp_info.get("pre_epsilon", 0.5)
        anon_config["alg"] = dp_info.get("algorithm", "mst")

        if(dp_info.find("ignore")):
            for column in dp_info.find("ignore").findall("column"):
                ignore.append(column.get("name"))

        if(dp_info.find("categorical")):
            for column in dp_info.find("categorical").findall("column"):
                cat.append(column.get("name"))

        if(dp_info.find("ordinal")):
            for column in dp_info.find("ordinal").findall("column"):
                ord.append(column.get("name"))

        if(dp_info.find("continuous")):
            for column in dp_info.find("continuous").findall("column"):
                cont.append(column.get("name"))

            if(column.get("bins") or column.get("lower") or column.get("upper")):
                cont_config.append(
                    {
                    "name": column.get("name"),
                    "bins": column.get("bins",10),
                    "lower": column.get("lower"),
                    "upper": column.get("upper"),
                    }
                )

        anon_config["hide"] = ignore
        anon_config["cat"] = cat
        anon_config["cont"] = cont
        anon_config["ord"] = ord

    sens = table.find("value_faking")
    if sens:
        for column in sens.findall("column"):
            sens_config.append(
                {
                    "name": column.get("name"),
                    "method": column.get("method"),
                    "locales": column.get("locales"),
                    "seed": column.get("seed", 0),
                }
            )

    return anon_config, sens_config, cont_config


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
        anon_config, sens_config, cont_config = configFromXML(table)
        anonymizeDB(jdbcConfig, anon_config, sens_config, cont_config, templatesPath)


if __name__ == "__main__":
    main()
