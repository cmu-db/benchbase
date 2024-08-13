from logging import root
import xml.etree.ElementTree as ET
import sys
import json
import re
data = json.loads(sys.argv[1])

tree = ET.parse('config/{}/sample_{}_config.xml'.format(sys.argv[3], sys.argv[2]))
root = tree.getroot()



for key, value in data.items():
    element = root.find(key)
    value = str(value)
    if key == "warmup":
        element = root.find("works/work/warmup")
        if element == None:
            element = root.find("works/work")
            newarmp = ET.SubElement(element, "warmup")
            newarmp.text = value
        else:
            element.text = value
    elif key == "time":
        element = root.find("works/work/time")
        if element != None:
            element.text = value
    # Check if loaderThreads is in the data
    elif key == 'loaderThreads':
        loader_threads_value = str(data['loaderThreads'])

        # Check if <loaderThreads> already exists
        element = root.find("loaderThreads")
        if element is None:
            # Create <loaderThreads> element at the top level (direct child of root)
            new_loader_threads = ET.SubElement(root, "loaderThreads")
            new_loader_threads.text = loader_threads_value
        else:
            # If it exists, update its value
            element.text = loader_threads_value
    elif element != None:
        if key == "url":
            element.text = element.text.replace("localhost", value)
            if "sslmode" in data:
                element.text = element.text.replace("disable", "require") if data["sslmode"] else element.text.replace(
                    "require", "disable")
        else:
            element.text = value

tree.write('config/{}/sample_{}_config.xml'.format(sys.argv[3], sys.argv[2]))
# python modifyconfig.py '{"url":"jdbc:postgresql://localhost:5433/yugabyte","username":"yugabyte","password":""}' seats