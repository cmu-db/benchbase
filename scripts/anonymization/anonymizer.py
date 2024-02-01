"""Module that handles the full Anonymization pipeline
"""
import sys
import xml.etree.ElementTree as ET


def rewriteFakeQueries(fakeValues, path):
    """Method that rewrites existing query templates based on sensitive value replacements

    Args:
        fakeValues (dict): Key-Value pairs of replacement values
        path (str): Path pointing to the query template
    """
    # TODO


def fakeColumn(dataset, col, locales, method, seed=0):
    """Method that generates fake values for columns that are considered sensitive

    Args:
        dataset (DataFrame): A Pandas dataframe holding anonymized data
        col (str): The name of the column
        locales (str[]): A list of locales
        method (str): A string matching the desired faker method
        seed (int, optional): Seed for the fake values. Defaults to 0.

    Returns:
        dict: Mapping from original to fake values
    """
    # TODO


def getTimestampColumns(dbTypeList):
    """A helper function that returns a list of indexes of timestamp-type columns

    Args:
        dbTypeList (any): A list of Database column metadata

    Returns:
        int[]: A list of indexes of timestamp-type columns
    """
    # TODO


def dfFromTable(curs, table):
    """Helper function that creates a pandas DataFrame from a jaydebe connection

    Args:
        curs (cursor): The connection cursor
        table (str): The name of the table

    Returns:
        Dataframe,int[]: The table as a DataFrame and the indexes of timestamp columns
    """
    # TODO


def populateAnonFromDF(curs, df, table, timestampIndexes):
    """Helper function to fill a DB table from a DataFrame

    Args:
        curs (cursor): The connection cursor
        df (DataFrame): Pandas DataFrame
        table (str): The name of the table
        timestampIndexes (int[]): A list of indexes of timestamp-type columns
    """
    # TODO


def getDroppableInfo(dropCols, dataset):
    """Helper function that saves droppable columns from anonymization

    Args:
        dropCols (str[]): A list of column names
        dataset (DataFrame): The dataset

    Returns:
        DataFrame,int[]: The saved columns as a DataFrame and a list of the original indexes of the columns
    """
    # TODO


def anonymize(dataset: str, anonConfig: dict, sensConfig: dict, templatesPath: str):
    """Function that handles the data anonymization step

    Args:
        dataset (DataFrame): A pandas DataFrame containing the data
        anonConfig (dict): Anonymization config
        sensConfig (dict): Sensitive value config
        templatesPath (str): The path pointing to the query templates

    Returns:
        DataFrame: An anonymized version of the original data
    """
    # TODO


def anonymizeDB(
    jdbcConfig: dict, anonConfig: dict, sensConfig: dict, templatesPath: str
):
    """Function that handles the necessary steps for anonymization.
    Includes starting the JVM, Anonymizing and pushing data to the DB

    Args:
        jdbcConfig (dict): JDBC connection information
        anonConfig (dict): Anonymization config
        sensConfig (dict): Sensitive value config
        templatesPath (str): The path pointing to the query templates
    """
    # TODO


def listFromElement(element):
    """Helper function creating an array of values from a XML Element

    Args:
        string (str): A string of values separated by comma (,)

    Returns:
        str[]: A list of values
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
        (dict,dict): The configuration for the anonymization and sensitive value handling
    """

    anonConfig = {}
    sensConfig = {}

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

    sens = table.find("sensitive")
    if sens:
        sensList = []
        for sensCol in sens.findall("column"):
            sensList.append(
                {
                    "name": sensCol.get("name"),
                    "method": sensCol.get("method"),
                    "locales": sensCol.get("locales"),
                    "seed": sensCol.get("seed", 0),
                }
            )
        sensConfig["cols"] = sensList

    return anonConfig, sensConfig


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
        anonConfig, sensConfig = configFromXML(table)
        anonymizeDB(jdbcConfig, anonConfig, sensConfig, templatesPath)


if __name__ == "__main__":
    main()
