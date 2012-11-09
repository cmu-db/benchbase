#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Wrapper for testing mixed workload vs pure OLTP and OLAP
workload.

Usage:
 ./run_mix_vs_single [--no-test]
 ./run_mix_vs_single -h | --help

Options:
    --no-test       Don't run oltpbenchmark,
                    use existing saved .res files.
    -h --help     Show this screen.
"""
from subprocess import check_call
from contextlib import contextmanager
import os
import sys
import pylab as p
import shutil

try:
    from docopt import docopt
except ImportError:
    print "You need docopt to specify options"

PATH_TO_OLTP = ".."
PATH_TO_PLOTTER = os.path.abspath("plot/")
sys.path.insert(0, PATH_TO_PLOTTER)


TEMPLATE = """<?xml version="1.0"?>
<parameters>
    
    <!-- Connection details -->
    <dbtype>postgresql</dbtype>
    <driver>org.postgresql.Driver</driver>
    <DBUrl>jdbc:postgresql://localhost:5432/tpcc</DBUrl>
    <username>tpcc</username>
    <password>tpcc</password>
    <isolation>TRANSACTION_READ_COMMITTED</isolation>
    
    <!-- Scale factor is the number of warehouses in TPCC -->
    <scalefactor>2</scalefactor>
    
    <!-- The workload -->
    <!-- Number of terminal per workload -->
    <terminals>4</terminals>
    <!-- Can be workload-specific -->
    <terminals bench="chbenchmark">2</terminals>
    
    <!-- Workload-specific options a marked with @bench=[workload_name] -->
    <!-- Workload-specific number of terminals -->
    <terminals bench="chbenchmark">2</terminals>
    
    <works >
        <work>
          <time>{time}</time>
          <rate>{rate}</rate>
          
          <ratelimited bench="chbenchmark">false</ratelimited>
          
          <weights bench="tpcc">45,43,4,4,4</weights>
          <weights bench="chbenchmark">3, 2, 3, 2 , 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5</weights>
        </work>
    </works>
{transaction_types}
</parameters>
"""

TPCC_QUERIES = """
    <transactiontypes bench="tpcc">
        <transactiontype>
            <name>NewOrder</name>
        </transactiontype>
        <transactiontype>
            <name>Payment</name>
        </transactiontype>
        <transactiontype>
            <name>OrderStatus</name>
        </transactiontype>
        <transactiontype>
            <name>Delivery</name>
        </transactiontype>
        <transactiontype>
            <name>StockLevel</name>
        </transactiontype>
    </transactiontypes> 
"""

CHBENCHMARK_QUERIES = """
    <transactiontypes bench="chbenchmark">
        <transactiontype>
            <name>Q1</name>
        </transactiontype>
        <transactiontype>
            <name>Q2</name>
        </transactiontype>
        <transactiontype>
            <name>Q3</name>
        </transactiontype>
        <transactiontype>
            <name>Q4</name>
        </transactiontype>
        <transactiontype>
            <name>Q5</name>
        </transactiontype>
        <transactiontype>
            <name>Q6</name>
        </transactiontype>
        <transactiontype>
            <name>Q7</name>
        </transactiontype>
        <transactiontype>
            <name>Q8</name>
        </transactiontype>
        <transactiontype>
            <name>Q9</name>
        </transactiontype>
        <transactiontype>
            <name>Q10</name>
        </transactiontype>
        <transactiontype>
            <name>Q11</name>
        </transactiontype>
        <transactiontype>
            <name>Q12</name>
        </transactiontype>
        <transactiontype>
            <name>Q13</name>
        </transactiontype>      
        <transactiontype>
            <name>Q14</name>
        </transactiontype>      
        <transactiontype>
        <!-- Needs optimization -->
            <name>Q15</name>
        </transactiontype>   
        <transactiontype>
            <name>Q16</name>
        </transactiontype>
        <transactiontype>
            <name>Q17</name>
        </transactiontype>   
        <transactiontype>
            <name>Q18</name>
        </transactiontype>   
        <transactiontype>
            <name>Q19</name>
        </transactiontype>
        <transactiontype>
            <name>Q20</name>
        </transactiontype>
        <transactiontype>
            <name>Q21</name>
        </transactiontype>
        <transactiontype>
            <name>Q22</name>
        </transactiontype>
    </transactiontypes> 
"""

from plot_latencies import ThroughputExtractor, LatencyExtractor

class WorkloadConfig(object):
    """Contains information on workload"""
    TIME = 20
    RATE = 10000
    CONFIG_NAME = os.path.abspath("config.xml")

    def __init__(self, workloads, query_ranges={}):
        self.workloads = workloads
        self.query_ranges = query_ranges

    @contextmanager
    def create_config(self):
        with open(self.CONFIG_NAME, 'w') as config:
            config.write(self._get_config())
        yield self.CONFIG_NAME
        os.remove(self.CONFIG_NAME)

    def _get_transtype_parameters(self):
        """Returns list of names of the transaction type variable"""
        return "\n".join([globals()["{0}_QUERIES".format(wrkld.upper())]\
                 for wrkld in self.workloads.split(",")])

    def _get_config(self):
        """Return content of the corresponding config file"""
        global TEMPLATE
        return TEMPLATE.format(
                time=self.TIME,
                rate=self.RATE,
                transaction_types=self._get_transtype_parameters(),
                )


CONFIGS = {'TPCC': WorkloadConfig('tpcc',
                     {'OLTP': (2, 6)},
                ),
        'CH': WorkloadConfig('chbenchmark',
                    {'OLAP': (2, 23)},
                ),
        'MIXED': WorkloadConfig('tpcc,chbenchmark',
                            {'OLTP': (2, 6),
                             'OLAP': (7, 28),
                            },
                ),
            }


def run_test(name, config):
    """Runs tpcc and returns throughput and latency extractor objects"""

    with config.create_config() as config_path:
        check_call(["./oltpbenchmark",
                        '-b', config.workloads,
                        '-c', config_path,
                        '--create=false',
                        '--load=false',
                        '--execute=true',
                        '-s', "5",
                        '-o', 'output',
                        '--histograms',
                        ])

        shutil.copyfile("output.raw", name + ".raw")


def create_latency_diagrams(data):
    """Creates latency diagrams"""
    latency_figure = p.figure(figsize=(20, 6), dpi=80)
    ymax = data['MIXED']['LATENCY'].get_ymax()

    for number, config_name in enumerate(('CH', 'MIXED')):
        config = CONFIGS[config_name]

        subplot = latency_figure.add_subplot(1, 2, number + 1)
        latency_results = \
                     data[config_name]['LATENCY'].\
                        extract(config.query_ranges['OLAP'])

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
                data[config_name]['THROUGHPUT'].\
                    extract(config.query_ranges['OLTP'])

        ThroughputExtractor.decorate_subplot(subplot,
                                            throughput_data,
                                            "Throughput",
                                            config_name)

    p.savefig("OLTP.svg")
    p.show()


@contextmanager
def chdir(dest):
    """To the dest and back again"""
    old_dir = os.getcwd()
    os.chdir(dest)
    yield
    os.chdir(old_dir)


def main():
    """Main runner"""

    if "docopt" in globals():
        arguments = docopt(__doc__)
    else:
        arguments = {}

    if arguments.get("--help"):
        print __doc__
        sys.exit(0)
    with chdir(PATH_TO_OLTP):
        results = {}
        for name, config in CONFIGS.items():
            if not arguments.get("--no-test"):
                run_test(name, config)
            results[name] = {'LATENCY': LatencyExtractor(name + ".raw"),
                            'THROUGHPUT': ThroughputExtractor(name + ".raw")}

        create_latency_diagrams(results)
        create_throughput_diagrams(results)

    raw_input("Press enter to EXIT")
    sys.exit(0)

if __name__ == "__main__":
    main()
