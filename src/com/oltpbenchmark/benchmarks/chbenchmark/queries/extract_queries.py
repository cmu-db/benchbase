# -*- coding: utf-8 -*-
"""
Extracts SQL queries from java files.

@author: alendit
"""
import re
import sqlparse

if __name__ == "__main__":
    for x in xrange(1, 23):
        with open("Q{0}.java".format(x), "r") as java_file:
            with open("query{0}.sql".format(x), "w") as query_file:
                sql = "".join(re.findall('\"(.*?)\"', java_file.read()))
                sql = sql.replace("\\n", " ")
                sql = sql.replace("\t", "")
                sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
                query_file.write(sql)
                
                

