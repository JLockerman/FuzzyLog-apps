#!/usr/bin/gnuplot
set terminal postscript eps color solid enh font "Helvetica"
set output 'multi.eps'

set size 1.5,2.15

set border lw 3


#set key out
#set key at 14,330 horizontal
set key right top
set key font ",33"
set key width -8

set xtics font ",36" offset 0,-1 scale 4 
set ytics font ",36" offset -1 scale 4
set format y "%3.0f K"
set format x "%2.0f"
set ylabel 'Throughput (requests/sec)' font ",36" offset -9
set origin 0,1.025
set size 1.5,0.9
set xrange [0:100]
set yrange [0:500]

set xlabel 'Percentage of multiappend' font ",36" offset 0,-2.5
plot "multiappend.txt" title "Aggr throughput of 2 clients" with errorlines ps 4 lt 6 lc rgb "#555555" linewidth 10
