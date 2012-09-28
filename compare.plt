set datafile separator ","
plot "output.res" using 1:3 with lines, \
"output_mix.res" using 1:3 with lines
