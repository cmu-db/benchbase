### Author: <lefred$inuits,be> modified by: <curino@mit.edu>

global mysql_user
mysql_user = os.getenv('DSTAT_MYSQL_USER') or os.getenv('USER')

global mysql_pwd
mysql_pwd = os.getenv('DSTAT_MYSQL_PWD')

global mysql_host
mysql_host = os.getenv('DSTAT_MYSQL_HOST')

global mysql_port
mysql_port = os.getenv('DSTAT_MYSQL_PORT')


print "in mysql plugin"
class dstat_plugin(dstat):
    """
    Plugin for MySQL NDB.
    """

    def __init__(self):
        self.name = 'mysql-ndb on ' + mysql_host +' ' + mysql_port
       	self.nick = ("Data_memory", "Index_memory", "REDO", "ndbnodecount", "DATA_MEMORY", "DISK_OPERATIONS", "DISK_RECORDS", "FILE_BUFFERS", "JOBBUFFER", "RESERVED", "TRANSPORT_BUFFERS")
       	self.vars = ("Data_memory", "Index_memory", "REDO", "ndbnodecount", "DATA_MEMORY", "DISK_OPERATIONS", "DISK_RECORDS", "FILE_BUFFERS", "JOBBUFFER", "RESERVED", "TRANSPORT_BUFFERS")
        
        
    def check(self): 
        global MySQLdb
        import MySQLdb
        try:
            print mysql_host, mysql_port, mysql_user, mysql_pwd
            self.db = MySQLdb.connect(host=mysql_host,port=int(mysql_port), user=mysql_user, passwd=mysql_pwd)
        except:
            raise Exception, 'Cannot interface with MySQL server'

    def extract(self):
        try:
            c = self.db.cursor()
            c.execute("select memory_type, avg(used) from ndbinfo.memoryusage group by memory_type;")
            lines = c.fetchall()
            for line in lines:
                if len(line) < 2: continue
                if line[0] in self.vars:
                    self.set2[line[0]] = int(line[1])

            c.execute("""select log_type, avg(used) from ndbinfo.logbuffers group by log_type;""")        
            lines = c.fetchall()
            for line in lines:
                if len(line) < 2: continue
                if line[0] in self.vars:
                    self.set2[line[0]] = int(line[1])
            
            c.execute("""select 'ndbnodecount' as var, count(*) from ndbinfo.nodes where status = 'STARTED';""")
            lines = c.fetchall()
            for line in lines:
                if len(line) < 2: continue
                if line[0] in self.vars:
                    self.set2[line[0]] = int(line[1])

            c.execute("""select resource_name, avg(used) from ndbinfo.resources group by resource_name;""")
            lines = c.fetchall()
            for line in lines:
                if len(line) < 2: continue
                if line[0] in self.vars:
                    self.set2[line[0]] = int(line[1])
                                                               
            for name in self.vars:
                self.val[name] = (self.set2[name] - self.set1[name]) * 1.0 / elapsed

            if step == op.delay:
                self.set1.update(self.set2)
                
        except Exception, e:
            for name in self.vars:
                self.val[name] = -1

# vim:ts=4:sw=4:et

