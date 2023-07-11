#!/usr/bin/python

import json
import sys
import yaml
from jinja2 import Environment, FileSystemLoader
from functools import reduce
from operator import getitem


def set_nested_item(dataDict, mapList, val):
    """Set item in nested dictionary"""
    reduce(getitem, mapList[:-1], dataDict)[mapList[-1]] = val
    return dataDict


def jinja2_modification(context, input_yaml):
    template = Environment(loader=FileSystemLoader("./")).get_template(input_yaml)
    with open(input_yaml, 'w') as f:
        f.write(template.render(context))


def main():
    context = sys.argv[1]
    yaml_file = sys.argv[2]
    data = json.loads(context, strict=False)
    jinja2_modification(data, yaml_file)
    with open(yaml_file) as f:
        doc = yaml.load(f, Loader=yaml.FullLoader)
        for key, value in data.items():
            if key == "warmup":
                doc["works"]["work"]["warmup"] = value
            elif key == "time":
                doc["works"]["work"]["time_secs"] = value
            elif key == "url":
                doc[key] = doc[key].replace("localhost", value)
                if "sslmode" in data:
                    doc[key] = doc[key].replace("disable", "require") if data["sslmode"] else doc[key].replace(
                        "require", "disable")
            elif key == "setAutoCommit":
                doc["microbenchmark"]["properties"]["setAutoCommit"] = value
            else:
                nested_key = key.split('.')
                set_nested_item(doc, nested_key, value)

        with open(yaml_file, 'w') as fnew:
            yaml.safe_dump(doc, fnew, encoding='utf-8', allow_unicode=True)
    return


main()
