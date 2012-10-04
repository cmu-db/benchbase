#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 12:30:54 2012

@author: alendit
"""
import numpy as np
import pylab as p
import sys

if __name__ == '__main__':
    
    raw = np.genfromtxt(sys.argv[1], delimiter=",")
    raw = raw[1:]
    queries = raw[:, 0]
    
    convert = 10 ** 6 # microseconds
    
    result = []
    
    for q in set(queries):
        l_q = int(q)
        query_lat = raw[:,2][queries == q]
        l_count = len(query_lat)
        l_mean = query_lat.mean() / convert
        l_min = query_lat.min() / convert
        l_max = query_lat.max() / convert
        print locals()['l_q']
        
        print "{l_q} {l_count}\n{l_mean} {l_min} {l_max}".format(**locals())
        result.append([l_q, l_count, l_mean, l_min, l_max])
        
    result = np.array(result)
    
    
    
    fig = p.figure()
    
    ax = fig.add_subplot(111)
    
    x = result[:, 0]
    
    y = result[:, 2]
    
    width = .2
    
    
    ax.bar(x, y, width=width, color='yellow')
    ax.bar(x+width, result[:, 3], width=width, color='green')
    ax.bar(x+width*2, result[:, 4], width=width, color='red')    
    
    ax.set_ylabel("Seconds")
    ax.set_xlabel("Query Number")
    
    ax.set_ylim(ymax=1.5)
    
    
    ax.set_xticks(x + width * 1.5)
    ax.set_xticklabels(x.astype('I') - 1)
    
    if len(sys.argv) > 2:
        title = sys.argv[2]
        ax.set_title(title)
        p.savefig(title)
        
    p.show()
    
