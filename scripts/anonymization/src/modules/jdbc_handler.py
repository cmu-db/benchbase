"""JDBCHandler. A class that handles all things related to database connections
"""

import jpype
import jpype.imports
import jaydebeapi
import pandas as pd
import numpy as np


class JDBCHandler:
    """
    A class to represent a JDBC connection handler

    Attributes
    ----------
    driver : str
        jdbc driver
    url : str
        database url
    username : str
        database username
    password : str
        database password
    """

    JAR_PATH = "benchbase.jar"

    def __init__(self, driver: str, url: str, username: str, password: str):
        self.driver = driver
        self.url = url
        self.username = username
        self.password = password

    def start_jvm(self):
        """Function that starts the Java Virtual Machine based on the JAR file created by BenchBase"""
        jpype.startJVM(classpath=[self.JAR_PATH])

    def get_connection(self):
        """Function that returns a database connection based on the class attributes

        Returns:
            jaydebeapi.Connection: A connection to the database service
        """
        return jaydebeapi.connect(self.driver, self.url, [self.username, self.password])

    def data_from_table(self, conn: jaydebeapi.Connection, table: str):
        """Function that pulls data from a specific table of the database

        Args:
            conn (jaydebeapi.Connection): Connection to the database
            table (str): Name of the table

        Returns:
            (pd.DataFrame,list[int]): The table as a DataFrame and a list of indexes for all time-related columns
        """
        curs = conn.cursor()
        curs.execute(f"SELECT * FROM {table}")

        res = curs.fetchall()
        meta = curs.description
        curs.close()

        cols = []
        col_types = []
        for entry in meta:
            cols.append(str(entry[0]))
            col_types.append(entry[1])
        timestamp_indexes = self.__get_timestamp_indexes(col_types)

        frame = pd.DataFrame(res, columns=cols)

        return frame, timestamp_indexes

    def __get_timestamp_indexes(self, col_types: list):
        """Function that analyzes table metadata and returns a list of indexes of time-related columns

        Args:
            col_types (list): A list of column types

        Returns:
            list[int]: A list of indexes
        """
        indexes = []
        for i, entry in enumerate(col_types):
            for d_type in entry.values:
                if d_type in ("TIMESTAMP", "DATE", "TIME"):
                    indexes.append(i)
        return indexes

    def create_anonymized_table(self, conn: jaydebeapi.Connection, table: str):
        """Function that creates an empty copy of an existing table on the database

        Args:
            conn (jaydebeapi.Connection): Connection to the database
            table (str): Name of the original table

        Returns:
            str: The name of the copied version
        """
        curs = conn.cursor()
        anon_table_name = table + "_anonymized"
        curs.execute(f"DROP TABLE IF EXISTS {anon_table_name}")
        curs.execute(f"CREATE TABLE {anon_table_name} AS TABLE {table} WITH NO DATA")
        curs.close()
        return anon_table_name

    def populate_anonymized_table(
        self,
        conn: jaydebeapi.Connection,
        df: pd.DataFrame,
        table: str,
        timestamp_indexes,
    ):
        """Function that pushed data to a table on the database

        Args:
            conn (jaydebeapi.Connection): Connection to the database
            df (pd.DataFrame): Data to push
            table (str): Name of the table that receives the data
            timestamp_indexes (list[int]): A list of indexes of time-related
        """
        # NaN replacement with NONE
        df = df.replace(np.nan, None)

        # Parsing timestamps to datetime format
        for ind in timestamp_indexes:
            name = df.columns[ind]
            df[name] = pd.to_datetime(df[name], format="mixed")

        tuples = [tuple(x) for x in df.values]

        if len(timestamp_indexes):

            # This is a dynamic import that only works once the JVM is running
            import java  # pylint: disable=import-outside-toplevel,import-error

            for i, tup in enumerate(tuples):
                li = list(tup)
                for j in timestamp_indexes:
                    if pd.isnull(li[j]):
                        li[j] = None
                    else:
                        li[j] = java.sql.Timestamp @ li[j]
                tuples[i] = tuple(li)

        column_slots = f"({','.join('?' for _ in df.columns)})"
        insert_query = f"insert into {table} values {column_slots}"

        curs = conn.cursor()
        curs.executemany(insert_query, tuples)
        curs.close()
