#!/bin/sh
gnuplot << EOF
set datafile separator ','
set terminal png 
set output "$1.png"
set xlabel "Time [Seconds]"
set ylabel "Throughput [TPs]"
set title "TODO"
plot "$1" using 1:2 t "MySQL" w l
EOF
