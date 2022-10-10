#!/usr/bin/python

import json
import sys
import yaml


def main():
    data = json.loads(sys.argv[1])
    with open(sys.argv[2]) as f:
        doc = yaml.load(f, Loader=yaml.FullLoader)
        for key, value in data.items():
            doc[key] = value
            print(key, value)

        with open(sys.argv[2], 'w') as fnew:
            yaml.safe_dump(doc, fnew, encoding='utf-8', allow_unicode=True)
    return


main();
