#!/usr/bin/gnuplot
set terminal postscript eps color solid enh font "Helvetica"
set output 'scalability.eps'

set size 2.0,2.5

set border lw 3

set title "Aggregate throughput of async put\n(window size = 32, 1 lock server, 3 log servers)" font ",36" offset 0,3

#set key out
#set key at 14,330 horizontal
set key right top
set key font ",33"
set key width -8
set nokey

set xtics font ",36" offset 0,-1 scale 4 
set ytics font ",36" offset -1 scale 4
set format y "%4.0f K"
set format x "%2.0f"
set ylabel 'Throughput (requests/sec)' font ",32" offset -7
set origin 0,1.025
set size 1.5,1.5
set xrange [0:16]
set yrange [0:200]

set xlabel 'Num of clients' font ",32" offset 0,-2.5
plot "scalability.txt" with errorlines ps 4 lt 6 lc rgb "#555555" linewidth 10
