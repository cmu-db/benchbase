#!/usr/bin/env python
# -*- coding: utf-8 -*-
from subprocess import check_call, STDOUT
import os

PATH_TO_OLTP = ".."

if __name__ == "__main__":
    configs = [#('tpcc', 'config/tpcc_config_postres.xml'),
                #('chbenchmark', 'config/chbench_config_postgres.xml'),
                ('tpcc,chbenchmark', 'config/mix_config_postgres.xml'),
                ]

    for config in configs:
        old_dir = os.getcwd()
        os.chdir(PATH_TO_OLTP)
        print config
        call_args = ["./oltpbenchmark",
        '-b', config[0],
        '-c', config[1],
        '--create=false',
        '--load=false',
        '--execute=true',
        '-s', "5",
        '-o', 'output',
        '--histograms']
        print " ".join(call_args)
        check_call(call_args)

