#!/usr/bin/python

"""Averages the per transaction latency for TPC-C traces."""

import sys

if __name__ == "__main__":
    start = False

    count = 0
    sum = 0
    for line in sys.stdin:
        if not start:
            start = line.startswith("Transaction Number")
        elif start:
            if line == "\n": break
            parts = line.split("\t")
            try:
                latency = int(parts[3])
            except:
                print repr(line)
                raise

            sum += latency
            count += 1

    print "%d / %d = %f average" % (sum, count, float(sum) / count)
