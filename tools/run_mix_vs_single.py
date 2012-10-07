#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Wrapper for testing mixed workload vs pure OLTP and OLAP
workload
"""
from subprocess import check_call
import os
import sys
import pylab as p

PATH_TO_OLTP = ".."
PATH_TO_PLOTTER = os.path.abspath("plot/")
sys.path.insert(0, PATH_TO_PLOTTER)

from plot_latencies import ThroughputExtractor, LatencyExtractor

CONFIGS = {'TPCC': ('tpcc', 'config/tpcc_config_postgre.xml',
                     {'OLTP': (2, 6)},
                 ),
        'CH': ('chbenchmark', 'config/hc_config_postgres.xml',
                    {'OLAP': (2, 23)},
                ),
        'MIXED': ('tpcc,chbenchmark', 'config/mix_config_postgres.xml',
                    {'OLTP': (2, 6),
                    'OLAP': (7, 28),
                    },
                ),
            }


def run_test(config):
    """Runs tpcc and returns throughput and latency extractor objects"""

    check_call(["./oltpbenchmark",
                    '-b', config[0],
                    '-c', config[1],
                    '--create=false',
                    '--load=false',
                    '--execute=true',
                    '-s', "5",
                    '-o', 'output',
                    '--histograms',
                    ])

    return {'THROUGHPUT': ThroughputExtractor("output.raw"),
            'LATENCY': LatencyExtractor("output.raw"),
            }


def create_latency_diagrams(data):
    """Creates latency diagrams"""
    latency_figure = p.figure(figsize=(20, 6), dpi=80)
    ymax = data['MIXED']['LATENCY'].get_ymax()

    for number, config_name in enumerate(('CH', 'MIXED')):
        config = CONFIGS[config_name]

        subplot = latency_figure.add_subplot(1, 2, number + 1)
        latency_results = \
                     data[config_name]['LATENCY'].extract(config[2]['OLAP'])

        LatencyExtractor.decorate_subplot(subplot,
                                 latency_results,
                                 ymax=ymax,
                                 title=config_name,
                                 )
    p.savefig("OLAP.svg")
    p.show()


def create_throughput_diagrams(data):
    """Creates throughput diagrams"""

    throughput_figure = p.figure()
    subplot = throughput_figure.add_subplot(111)

    for config_name in ('TPCC', 'MIXED'):
        config = CONFIGS[config_name]

        throughput_data = \
                data[config_name]['THROUGHPUT'].extract(config[2]['OLTP'])

        ThroughputExtractor.decorate_subplot(subplot,
                                            throughput_data,
                                            "Throughput",
                                            config_name)

    p.savefig("OLTP.svg")
    p.show()


def main():
    """Main runner"""

    old_dir = os.getcwd()
    os.chdir(PATH_TO_OLTP)
    results = dict([(name, run_test(config)) \
                    for name, config in CONFIGS.items()])
    os.chdir(old_dir)

    create_latency_diagrams(results)
    create_throughput_diagrams(results)

    raw_input("Press enter to EXIT")
    sys.exit(0)

if __name__ == "__main__":
    main()
