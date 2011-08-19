#!/usr/bin/python

import csv
import os.path
import sys

import stupidplot


def CSVIterator(path):
    """Returns an iterator over rows in path. If path is '-' it reads from stdin."""
    if path == "-":
        data = sys.stdin
    else:
        data = open(path)

    reader = csv.reader(data)

    for row in reader:
        out = []
        for v in row:
            try:
                out.append(float(v))
            except ValueError:
                pass
        yield out

    data.close()


def plotScatter(scatter_data, output_path):
    options = {
        'plottype': 'points',
        'key': False,
        #~ 'ylabel': scatter_block[0][1],
        'yformat': '%g',
        'xformat': '%g',
    }
    stupidplot.gnuplotTable(scatter_data, output_path, options)


def plotAverage(average_data, output_path):
    options = {
        #~ 'plottype': 'points',
        #~ 'key': False,
        #~ 'ylabel': 'Transactions/s',
        'errorbars': [1],
        'yformat': '%g',
        'xformat': '%g',
        #~ 'xrange' : "[0:]",
        'yrange' : "[0:]",
    }
    stupidplot.gnuplotTable(average_data, output_path, options)


def main():
    if len(sys.argv) < 3:
        sys.stderr.write("plot_raw.py [input file|- for stdin]+ [output prefix]\n")
        sys.exit(1)

    input_paths = sys.argv[1:-1]
    output_prefix = sys.argv[-1]

    output_throughput = output_prefix + "_thrpt.eps"
    output_latency = output_prefix + "_ltncy.eps"
    error = False
    for path in (output_throughput, output_latency):
        if os.path.exists(path):
            sys.stderr.write("output path '%s' exists; please move it out of the way\n" % path)
            error = True
    if error:
        sys.exit(1)


    throughputs = []
    latencies = []
    for input_path in input_paths:
        throughput = [("time (s)", input_path)]
        latency = [("time (s)", input_path)]
        for r in CSVIterator(input_path):
            start = r[0]
            sample_average = r[1]
            sample_throughput = r[2]

            throughput.append([start, sample_throughput])
            latency.append([start, sample_average])

        throughputs.append(throughput)
        latencies.append(latency)

    options = {
        'yformat': '%g',
        'xformat': '%g',
        'yrange' : "[0:]",
    }
    options["ylabel"] = "Throughput (txns/s)"
    stupidplot.gnuplotTable(throughputs, output_throughput, options)
    options["ylabel"] = "Latency (us)"
    #~ options["yrange"] = "[:1000000]"
    stupidplot.gnuplotTable(latencies, output_latency, options)


if __name__ == "__main__":
    main()

