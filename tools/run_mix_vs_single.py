#!/usr/bin/env python
# -*- coding: utf-8 -*-
from subprocess import check_call, STDOUT, Popen
import os, sys

PATH_TO_OLTP = ".."
PATH_TO_PLOTTER = os.path.abspath("plot/")
sys.path.insert(0, PATH_TO_PLOTTER)
from plot_latencies import ThroughputExtractor, LatencyExtractor

def main():
    """Main runner"""
    configs = [#('tpcc', 'config/tpcc_config_postgre.xml'),
                #('chbenchmark', 'config/hc_config_postgres.xml'),
                ('tpcc,chbenchmark', 'config/mix_config_postgres.xml'),
                ]

    old_dir = os.getcwd()
    os.chdir(PATH_TO_OLTP)
    results = []
    
    for config in configs:
        call_args = ["./oltpbenchmark",
        '-b', config[0],
        '-c', config[1],
        '--create=false',
        '--load=false',
        '--execute=true',
        '-s', "5",
        '-o', 'output',
        '--histograms']
        print check_call(call_args)
        
        throughput_extractor = ThroughputExtractor("output.raw")
        latency_extranctor = LatencyExtractor("output.raw")
        results.append((throughput_extractor, latency_extranctor))
                    

    raw_input("Press enter to EXIT")
    sys.exit(0)

if __name__ == "__main__":
    main()    
