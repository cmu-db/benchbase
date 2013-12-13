# Reads a file output of TPC-H's qgen and outputs .sql files

# @author: ipandis

def writeSqlFile(n):
	number = "{0}".format(x).zfill(2)
	sqlFileName = "Q{0}.sql".format(number)
    	with open(sqlFileName, "w") as sqlFile:
    	 print "Writing {0}".format(sqlFileName)
    	 print >> sqlFile, "-- TPC-H Q{0}".format(number)

for x in xrange(01, 22):
	writeSqlFile(x)
	
##for num, pre in enumerate(hxs.select('//pre[span[contains(text(), "select")]]')):
##	with open("q{0}.txt".format(num + 1), "w") as q:
##		print >>q, " ".join(pre.select(".//text()").extract())
    
