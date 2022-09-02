from logging import root
import xml.etree.ElementTree as ET
import sys
import json


tree = ET.parse('config/yugabyte/sample_{}_config.xml'.format(sys.argv[2]))
root = tree.getroot()

data=json.loads(sys.argv[1])
for key,value in data.items():
    element=root.find(key)
    if element!=None :
     element.text=value
    print(key,value)


tree.write('config/yugabyte/sample_{}_config.xml'.format(sys.argv[2]))


# python modifyconfig.py '{"url":"jdbc:postgresql://localhost:5433/yugabyte","username":"yugabyte","password":""}' seats