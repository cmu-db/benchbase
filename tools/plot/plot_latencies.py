#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 12:30:54 2012

@author: alendit
"""
import numpy as np
import pylab as p
import sys

SLICE_SIZE = 5

class LatencyExtractor(object):
    """Analyser for output.raw for latency data"""

    CONVERT = 10 ** 6 # microseconds

    def __init__(self, filename, output=None, interval=None):
        self.filename = filename
        self.output = output
        self.interval = interval

    def extract(self):
        """Parses output.raw and extracts latency data"""
        raw = np.genfromtxt(self.filename, delimiter=",")
        # remove first line as it is invalid
        raw = raw[1:]
        queries_column = raw[:, 0]
        queries = np.unique(queries_column)
        if self.interval:
            first, last = self.interval
            interval_queries = xrange(first, last + 1)
            assert set(interval_queries).issubset(queries)
            queries = np.array(interval_queries)
        
        result = []
        
        for query in queries:
            l_q = int(query)
            query_lat = raw[:, 2][queries_column == query]
            result.append([l_q] + self._get_data(query_lat))

        return np.array(result)

    def _get_data(self, data):
        """Returns count, mean, min and max from a array"""
        return [len(data), data.mean() / self.CONVERT, 
                data.min() / self.CONVERT, data.max() / self.CONVERT]

    def plot(self, data, ymax=1.5):
        """Takes latency data and plots bar charts"""
        fig = p.figure()
        
        subplot = fig.add_subplot(111)
        
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
        subplot.set_xticklabels(queries.astype('I') - 1)
        
        if self.output:
            subplot.set_title(self.output)
            p.savefig(self.output)
            
        p.show()

class TroughputExtractor(object):
    """Analyser for output.raw for troughput data"""

    def __init__(self, filename, output=None, interval=None):
        self.filename = filename
        self.output = output
        self.interval = interval

    def extract(self):
        """Parses output.raw and returns troughput data"""

        raw = np.genfromtxt(self.filename, delimiter=',')
        # filter the invalid transaction
        raw = raw[1:]
        if self.interval:
            first, last = self.interval
            raw = raw[(raw[:, 0] >= first) & (raw[:, 0] <= last)]
        test_start = raw[:, 1].min()
        result = []

        for time in xrange(0, 61, 5):
            start = time
            end = time + SLICE_SIZE
            time_slice = raw[(raw[:, 1] >= start + test_start) 
                                & (raw[:, 1] < end + test_start)][:, 2]
            throughput = len(time_slice) / SLICE_SIZE
            result.append(throughput)

        return np.array(result)


    def plot(self, data):
        """Takes throughput data and plots it"""
        fig = p.figure()

        subplot = fig.add_subplot(111)

        time_intervals = np.arange(len(data))

        subplot.plot(time_intervals, data)

        subplot.set_xticks(time_intervals)
        subplot.set_xticklabels([time_interval * 5 \
                    for time_interval in time_intervals])
        if self.output:
            subplot.title(self.output)
            p.savefig(self.output)

        p.show()

def main():
    """Script runner"""
    latency_extractor = LatencyExtractor(sys.argv[1],
                            sys.argv[2] if len(sys.argv) > 2 else None)
   
    latency_data = latency_extractor.extract()
    ymax = latency_data[:, 4].max() + .1
    latency_extractor.plot(latency_data, ymax)

if __name__ == '__main__':
    main()