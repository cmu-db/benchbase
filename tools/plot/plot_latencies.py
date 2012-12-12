#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 12:30:54 2012

@author: alendit
"""
import numpy as np
import pylab as p
from matplotlib.ticker import MaxNLocator
import sys

SLICE_SIZE = 5
MAX_THROUGHPUT_TICKS = 10


class LatencyExtractor(object):
    """Analyser for output.raw for latency data"""

    CONVERT = 10 ** 6  # microseconds

    def __init__(self, filename, output=None):
        self.filename = filename
        self.output = output
        self.raw = np.genfromtxt(self.filename, delimiter=',')
        # remove first line as it is invalid
        self.raw = self.raw[1:]

    def extract(self, interval=None):
        """Parses output.raw and extracts latency data"""
        queries_column = self.raw[:, 0]
        queries = np.unique(queries_column)
        if interval:
            first, last = interval
            interval_queries = xrange(first, last + 1)
            queries = set(interval_queries).intersection(queries)

        result = []

        for query in queries:
            l_q = int(query)
            query_lat = self.raw[:, 2][queries_column == query]
            result.append([l_q] + self._get_data(query_lat))

        return np.array(result)

    def _get_data(self, data):
        """Returns count, mean, min and max from a array"""
        return [len(data), data.mean() / self.CONVERT,
                data.min() / self.CONVERT, data.max() / self.CONVERT]

    def get_ymax(self):
        """Get max latency value for deminsioning of the y axis"""
        return self.raw[:, 2].max() / self.CONVERT + .1

    def plot(self, data, ymax=1.5):
        """Takes latency data and plots bar charts"""
        fig = p.figure()

        subplot = fig.add_subplot(111)

        title = getattr(self, 'title', None)
        LatencyExtractor.decorate_subplot(subplot, data, ymax, title)

        if self.output:
            subplot.set_title(self.output)
            p.savefig(self.output)

        p.show()

    @staticmethod
    def decorate_subplot(subplot, data, ymax, title=None):
        """Takes a subplot and adds a latency barchart to it"""
        queries = data[:, 0]

        width = .2

        subplot.bar(queries, data[:, 2], width=width, color='yellow')
        subplot.bar(queries + width, data[:, 3], width=width, color='green')
        subplot.bar(queries + width * 2, data[:, 4], width=width,
                    color='red')

        subplot.set_ylabel("Seconds")
        subplot.set_xlabel("Query Number")

        subplot.set_ylim(ymax=ymax)

        subplot.set_xticks(queries + width * 1.5)
        subplot.set_xticklabels((queries - queries.min()).astype("I") + 1)

        if title:
            subplot.set_title(title)


class ThroughputExtractor(object):
    """Analyser for output.raw for throughput data"""

    def __init__(self, filename, output=None):
        self.filename = filename
        self.output = output
        raw = np.genfromtxt(self.filename, delimiter=',')
        # filter the invalid transaction
        self.raw = raw[1:]

    def extract(self, interval=None):
        """Parses output.raw and returns throughput data"""

        raw = np.genfromtxt(self.filename, delimiter=',')
        # filter the invalid transaction
        raw = raw[1:]
        if interval:
            first, last = interval
            raw = self.raw[(self.raw[:, 0] >= first) & \
                            (self.raw[:, 0] <= last)]
        else:
            raw = self.raw
        test_start = raw[:, 1].min()
        test_finish = raw[:, 1].max()
        result = []

        for time in xrange(0, int(test_finish - test_start), 5):
            start = time
            end = time + SLICE_SIZE
            time_slice = raw[(raw[:, 1] >= start + test_start)
                             & (raw[:, 1] < end + test_start)][:, 2]
            throughput = float(len(time_slice)) / SLICE_SIZE
            result.append(throughput)

        return np.array(result)


    def plot(self, data):
        """Takes throughput data and plots it"""
        fig = p.figure()
        title = getattr(self, 'title', None)

        subplot = fig.add_subplot(111)
        ThroughputExtractor.decorate_subplot(subplot, data, title)

        if title:
            p.savefig(self.output)

        p.show()

    @staticmethod
    def decorate_subplot(subplot, data, title=None, label=None):
        """Takes a subplot and adds graph to it"""

        time_intervals = (np.arange(len(data)) * SLICE_SIZE).astype("I")

        subplot.plot(time_intervals, data, label=label)

        subplot.set_xticks(time_intervals)

        subplot.xaxis.set_major_locator(MaxNLocator(MAX_THROUGHPUT_TICKS))

        subplot.set_xlabel("Seconds")
        subplot.set_ylabel("Requests/s")

        subplot.legend(*subplot.get_legend_handles_labels())
        if title:
            subplot.set_title(title)


def main():
    """Script runner"""
    latency_extractor = LatencyExtractor(sys.argv[1],
                    sys.argv[2] if len(sys.argv) > 2 else None)
    latency_data = latency_extractor.extract()
    ymax = latency_extractor.get_ymax()
    latency_extractor.plot(latency_data, ymax)

    throughput_extractor = ThroughputExtractor(sys.argv[1],
                    sys.argv[2] if len(sys.argv) > 2 else None)
    throughput_extractor.plot(throughput_extractor.extract())

if __name__ == '__main__':
    main()
