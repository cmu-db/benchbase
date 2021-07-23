"""
python3 result_aggregator.py path_to_results_with_summary_json

Produces a nicely formatted message for GitHub Actions.
"""

import glob
import json
import os
import sys


if __name__ == '__main__':
  os.chdir(sys.argv[1])
  dbms_version = ''
  output = []
  for summary_json_file in sorted(glob.glob("*.summary.json")):
    with open(summary_json_file) as summary_json:
      data = json.load(summary_json)
      dbms = str(data['DBMS Version']).strip()
      benchmark = str(data['Benchmark Type']).strip()
      throughput = str(data['Throughput (requests/second)']).strip()
      assert dbms_version == '' or dbms_version == dbms
      dbms_version = dbms
      output.append((benchmark, throughput))
  print(dbms_version, end='<br>')
  for benchmark, throughput in output:
    print(benchmark + ', ' + throughput, end='<br>')
