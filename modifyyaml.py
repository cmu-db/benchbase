#!/usr/bin/python

import json
import sys
import yaml
from jinja2 import Environment, FileSystemLoader


def jinja2_modification(context, input_yaml):
    template = Environment(loader=FileSystemLoader("./")).get_template(input_yaml)
    with open(input_yaml, 'w') as f:
        f.write(template.render(context))


def main():
    context = sys.argv[1]
    yaml_file = sys.argv[2]
    data = json.loads(context)
    jinja2_modification(data, yaml_file)
    data = json.loads(context)
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
                doc[key] = value
            print(key, value)

        with open(yaml_file, 'w') as fnew:
            yaml.safe_dump(doc, fnew, encoding='utf-8', allow_unicode=True)
    return


main()
