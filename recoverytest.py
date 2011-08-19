#!/usr/bin/python

"""Script to measure the time it takes MySQL to recover after a failure."""

import errno
import socket
import subprocess
import time

MYSQL_DIR = "/home/evanj/mysql-5.5.6-rc-linux2.6-i686"
DATA_DIR = "/home/evanj/tpcc_mysql"
PORT = 3306

COMMAND = (
    MYSQL_DIR + "/bin/mysqld",
    "--no-defaults",
    "--basedir=" + MYSQL_DIR,
    "--datadir=" + DATA_DIR,
    "--tmpdir=" + DATA_DIR,
    "--socket=mysql.sock",
    "--pid-file=mysql.pid",
    "--innodb-buffer-pool-size=1G",
    "--innodb_log_file_size=256M",
    #~ "--innodb_flush_method=O_DIRECT",
)

CLIENT = (
    "mysql",
    "--user=root",
    "--socket=" + DATA_DIR + "/mysql.sock")


if __name__ == "__main__":
    process = subprocess.Popen(COMMAND)

    start = time.time()
    connected = False
    while not connected:
        try:
            s = socket.socket()
            s.connect(('', PORT))
            connected = True
        except socket.error, e:
            assert e.args[0] == errno.ECONNREFUSED
        finally:
            s.close()

    # Test that the connection actually works (it does, in my experience)
    #~ client = subprocess.Popen(CLIENT, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    #~ client.stdin.write("show status like 'Uptime';\n")
    #~ client.stdin.close()
    #~ out = client.stdout.read()

    end = time.time()
    
    #~ print out
    #~ code = client.wait()
    #~ assert code == 0

    process.terminate()
    code = process.wait()
    assert code == 0

    print "elapsed recovery time = " + str(end - start)
