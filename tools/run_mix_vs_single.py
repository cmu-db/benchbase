#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Wrapper for testing mixed workload vs pure OLTP and OLAP
workload
"""
from subprocess import check_call
import os, sys
import pylab as p

PATH_TO_OLTP = ".."
PATH_TO_PLOTTER = os.path.abspath("plot/")
sys.path.insert(0, PATH_TO_PLOTTER)

from IPython import embed

from plot_latencies import ThroughputExtractor, LatencyExtractor

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
                        
    return {'THROUGHPUT' : ThroughputExtractor("output.raw"),
            'LATENCY' : LatencyExtractor("output.raw"),
            }

def main():
    """Main runner"""

    configs = {
            # 'TPCC' : ('tpcc', 'config/tpcc_config_postgre.xml', 
            #             {'OLTP' : (2, 6)},
            #         ),
            'CH' : ('chbenchmark', 'config/hc_config_postgres.xml',
                        {'OLAP' : (2, 23)},
                    ),
            'MIXED' : ('tpcc,chbenchmark', 'config/mix_config_postgres.xml',
                        {'OLTP' : (2, 6),
                        'OLAP' : (7, 28),
                        },
                    ),
                }

    old_dir = os.getcwd()
    os.chdir(PATH_TO_OLTP)
    results = dict([(name, run_test(config)) \
                    for name, config in configs.items()])

    # create latency diagrams

    figure = p.figure() 
    ymax = results['MIXED']['LATENCY'].get_ymax()

    for number, config_name in enumerate(('CH', 'MIXED')):
        config = configs[config_name]

        subplot = figure.add_subplot(1, 2, number + 1)
        latency_results = \
                     results[config_name]['LATENCY'].extract(config[2]['OLAP'])

        LatencyExtractor.decorate_subplot(subplot,
                                 latency_results,
                                 ymax=ymax,
                                 title=config_name,
                                 )
    p.savefig("OLAP.svg")
    p.show()
    raw_input("Press enter to EXIT")
    os.chdir(old_dir)
    sys.exit(0)

if __name__ == "__main__":
    main()    
