#!/usr/bin/gnuplot
set terminal postscript eps color solid enh font "Helvetica"
set output 'scalability.eps'

set size 1.5,2.15

set border lw 3


#set key out
#set key at 14,330 horizontal
set key right top
set key font ",33"
set key width -8

set xtics font ",36" offset 0,-1 scale 4 
set ytics font ",36" offset -1 scale 4
set format y "%4.0f K"
set format x "%2.0f"
set ylabel 'Throughput (requests/sec)' font ",36" offset -9
set origin 0,1.025
set size 1.5,0.9
set xrange [0:32]
set yrange [0:1500]

set xlabel 'Num of clients' font ",36" offset 0,-2.5
plot "scalability.txt" title "Aggr throughput, 8 server threads" with errorlines ps 4 lt 6 lc rgb "#555555" linewidth 10
