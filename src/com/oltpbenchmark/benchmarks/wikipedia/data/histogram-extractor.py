#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
from datetime import datetime
from pprint import pprint,pformat
from decimal import Decimal
import MySQLdb as mdb

class Histogram(object):
    def __init__(self):
        self.data = { }
    def put(self, x, delta=1):
        self.data[x] = self.data.get(x, 0) + delta
    def get(self, x):
        return self.data[x]
    def toJava(self):
        output = ""
        for key in sorted(self.data.keys()):
            cnt = self.data[key]
            if type(key) == str:
                key = "\"%s\"" % (key.replace('"', '\\"'))
            output += "this.put(%s, %d);\n" % (key, cnt)
        return output
    ## DEF
## CLASS

## ==============================================
## extractHistograms
## ==============================================
def extractHistograms(histograms, tableName, len_fields=[], cnt_fields=[], custom_fields={}):
    all_fields = [ ]
    if len_fields:
        all_fields.append(", ".join([ "LENGTH(%s) AS %s" % (x, x) for x in len_fields ]))
    if cnt_fields:
        all_fields.append(", ".join(cnt_fields))
    if custom_fields:
        for key,val in custom_fields.items():
            all_fields.append("%s AS %s" % (val, key))
    sql = "SELECT %s FROM %s" % ( \
            ", ".join(all_fields), \
            tableName
    )
    print sql
    c1.execute(sql)
    fields = len_fields + cnt_fields + custom_fields.keys()
    num_fields = len(fields)
    for row in c1:
        for i in xrange(num_fields):
            f = fields[i]
            if not f in histograms: histograms[f] = Histogram()
            if type(row[i]) == Decimal:
                histograms[f].put(int(row[i]))
            else:
                histograms[f].put(row[i])
        ## FOR
    ## FOR
    return
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser()
    aparser.add_argument('--host', type=str, required=True, help='MySQL host name')
    aparser.add_argument('--name', type=str, required=True, help='MySQL database name')
    aparser.add_argument('--user', type=str, required=True, help='MySQL username')
    aparser.add_argument('--pass', type=str, required=True, help='MySQL password')
    args = vars(aparser.parse_args())
    
    mysql_conn = mdb.connect(host=args['host'], db=args['name'], user=args['user'], passwd=args['pass'])
    c1 = mysql_conn.cursor()
    c2 = mysql_conn.cursor()
    
    histograms = { }
    
    ## USER ATTRIBUTES
    fields = [ "user_name", "user_real_name", ]
    sql = """
        SELECT %s, COUNT(revision.rev_id) AS user_revisions, COUNT(watchlist.*) AS user_watches
          FROM user
          LEFT OUTER JOIN revision ON user.user_id = revision.rev_user
          LEFT OUTER JOIN watchlist ON user.user_id = watchlist.wl_user
         GROUP BY user_id
    """ % ",".join(fields)
    c1.execute(sql)
    fields.append("user_revisions")
    fields.append("user_watches")
    num_fields = len(fields)
    for row in c1:
        for i in xrange(num_fields):
            f = fields[i]
            if not f in histograms: histograms[f] = Histogram()
            if i+1 != num_fields:
                histograms[f].put(len(row[i]))
            else:
                histograms[f].put(int(row[i]))
        ## FOR
    ## FOR
    
    ## PAGE ATTRIBUTES
    len_fields = [ "page_title" ]
    cnt_fields = [ "page_namespace", "page_restrictions", "page_counter" ]
    extractHistograms(histograms, "page", len_fields, cnt_fields)
    
    ## REVISIONS PER PAGE
    sql = "SELECT COUNT(rev_id), rev_page FROM revision GROUP BY rev_page"
    c1.execute(sql)
    f = "rev_per_page"
    histograms[f] = Histogram()
    for row in c1:
        ## Round up
        cnt = row[0]
        if cnt >= 10000:
            cnt = round(cnt / 1000) * 1000
        elif cnt >= 1000:
            cnt = round(cnt / 100) * 100
        elif cnt >= 100:
            cnt = round(cnt / 10) * 10
        histograms[f].put(int(cnt))
    ## FOR
    
    ## REVISION ATTRIBUTES
    len_fields = [ "rev_comment" ]
    cnt_fields = [ "rev_minor_edit"  ]
    extractHistograms(histograms, "revision", len_fields, cnt_fields)
    
    ## TEXT ATTRIBUTES
    cnt_fields = [ "old_flags" ]
    custom_fields = { "old_text": "ROUND(LENGTH(old_text)/100.0)*100"}
    extractHistograms(histograms, "text", [], cnt_fields, custom_fields)
    
    c1.close()
    
    raw = { }
    for key in histograms.keys():
        print key
        print histograms[key].toJava()
        #raw[key] = histograms[key].data
    #pprint(raw)
## MAIN
    
    