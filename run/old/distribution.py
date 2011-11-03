#!/usr/bin/python

import csv
#~ import cPickle
import os.path
import sys

#~ import stupidplot


def CSVIterator(path):
    """Returns an iterator over rows in path. If path is '-' it reads from stdin."""
    if path == "-":
        data = sys.stdin
    else:
        data = open(path)

    reader = csv.reader(data)

    for row in reader:
        # Skip any trailing blank lines
        if len(row) == 0: continue

        out = []
        for v in row:
            try:
                out.append(float(v))
            except ValueError:
                pass
        yield out

    data.close()


class DistributionStatistics(object):
    __slots__ = ("average", "throughput", "percentiles")

    PERCENTILES = (0.0, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 1.0)

    def __init__(self, samples, time_secs):
        # Special case zero length samples
        if len(samples) == 0:
            self.average = 0
            self.throughput = 0
            self.percentiles = [0] * len(DistributionStatistics.PERCENTILES)
            return

        samples.sort()

        self.average = sum(samples) / float(len(samples))
        self.throughput = len(samples) / float(time_secs)

        # NOTE: NIST recommends interpolating. This just selects the closest value, which is
        # described as another common technique.
        # http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm
        self.percentiles = []
        for percentile_rank in DistributionStatistics.PERCENTILES:
            index = int(percentile_rank * len(samples))
            if index == len(samples):
                index -= 1
            self.percentiles.append(samples[index])

def timeBucket(samples, start_time_us, window_secs):
    windows = []
    window_start = start_time_us
    window_end = window_start + window_secs * 1000000.

    window_samples = []

    def addSamples():
        start_seconds = (window_start - start_time_us)/1000000.
        if len(window_samples) == 0:
            print "WARNING: empty window at start time =", start_seconds
        windows.append((start_seconds, DistributionStatistics(window_samples, window_secs)))
        #~ print start_seconds, len(window_samples), windows[-1][1].average, windows[-1][1].throughput

    for (start_us, latency_us) in samples:
        while window_end <= start_us:
            # Window has ended
            addSamples()

            window_start = window_end
            window_end = window_start + window_secs * 1000000
            window_samples = []
        window_samples.append(latency_us)

    if len(window_samples) > 0:
        # We used to add this sample, but it frequnetly led to skewed results due to an incomplete
        # bucket
        print "WARNING: discarding incomplete time bucket with %d samples; time range [%d, %d)" % (
                len(window_samples), (window_start - start_time_us)/1000000., (window_end  - start_time_us)/1000000.)
        #~ addSamples()

    return windows


def dumpCSV(statistics, output):
    for time, stat in statistics:
        output.write("%d,%f,%f," % (time, stat.average, stat.throughput))
        output.write(",".join((str(v) for v in stat.percentiles)))
        output.write("\n")


def main():
    if len(sys.argv) != 4:
        sys.stderr.write("distribution.py [window size (seconds)] [raw latency log csv] [output]\n")
        sys.exit(1)

    window_secs = int(sys.argv[1])
    input_path = sys.argv[2]
    output_path = sys.argv[3]

    if os.path.exists(output_path):
        sys.stderr.write("output path '%s' exists; please move it out of the way\n" % output_path)
        sys.exit(1)

    start_time_us = CSVIterator(input_path).next()[0]
    input_iterator = CSVIterator(input_path)
    # Drop the header
    #~ input_iterator.next()
    windowed = timeBucket(input_iterator, start_time_us, window_secs)
    
    output = open(output_path, "w")
    dumpCSV(windowed, output)
    output.close()


if __name__ == "__main__":
    main()

