import jpype
import jpype.imports
import jaydebeapi
import pandas as pd
import numpy as np


class JDBCHandler:
    JAR_PATH = "benchbase.jar"

    def __init__(self, driver: str, url: str, username: str, password: str):
        self.driver = driver
        self.url = url
        self.username = username
        self.password = password

    def start_JVM(self):
        jpype.startJVM(classpath=[self.JAR_PATH])

    def get_connection(self):
        return jaydebeapi.connect(self.driver, self.url, [self.username, self.password])

    def data_from_table(self, conn:jaydebeapi.Connection, table: str):
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
        timestamp_indexes = self.get_timestamp_indexes(col_types)

        frame = pd.DataFrame(res, columns=cols)

        return frame, timestamp_indexes
        

    def get_timestamp_indexes(self, col_types: list):
        indexes = []
        for i, entry in enumerate(col_types):
            for d_type in entry.values:
                if d_type == "TIMESTAMP" or d_type == "DATE" or d_type == "TIME":
                    indexes.append(i)
        return indexes

    def create_anonymized_table(self,conn:jaydebeapi.Connection,table:str):
        curs = conn.cursor()
        anon_table_name = table + "_anonymized"
        curs.execute(f"DROP TABLE IF EXISTS {anon_table_name}")
        curs.execute(f"CREATE TABLE {anon_table_name} AS TABLE {table} WITH NO DATA")
        curs.close()
        return anon_table_name

    def populate_anonymized_table(self, conn:jaydebeapi.Connection, df:pd.DataFrame, table: str, timestamp_indexes):
        # NaN replacement with NONE
        df = df.replace(np.nan, None)

        # Parsing timestamps to datetime format
        for ind in timestamp_indexes:
            name = df.columns[ind]
            df[name] = pd.to_datetime(df[name], format="mixed")

        tuples = [tuple(x) for x in df.values]

        if len(timestamp_indexes):
            import java

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
