#!/usr/bin/gnuplot
set terminal postscript eps color solid enh font "Helvetica"
set output 'crdt.eps'

set size 1.5,2.15

set border lw 3

set multiplot layout 2,1


set key out
set key at 14,330 horizontal
set key font ",33"
set key width -8

set xtics font ",36" offset 0,-1 scale 4 
set ytics font ",36" offset -1 scale 4
set format y "%3.0f K"
set format x "%2.0f"
set ylabel 'Throughput (requests/sec)' font ",36" offset -9
set origin 0,1.025
set size 1.5,0.9
set label 1 "(a) Total throughput" at 5,275 font ",33"
set xrange [0:17]
set yrange [0:250]

set xlabel 'Number of clients' font ",36" offset 0,-2.5
plot "d300000000.txt" title "No anti-entropy" with errorlines ps 4 lt 6 lc rgb "#5C3C1B" linewidth 10, \
 "d500000.txt" title "500 ms" with errorlines ps 4 lt 3 lc rgb "#FF345C" linewidth 10 



unset key
set origin 0,0.0
set size 1.5,0.9
set yrange [0:30]
set xrange [0:17]
unset label 1
set label 1 "(b) Per-client throughput" at 5,32 font ",33"
plot "d300000000_local.txt" title "No anti-entropy" with errorlines ps 4 lt 6 lc rgb "#5C3C1B" linewidth 10, \
 "d500000_local.txt" title "500 ms" with errorlines ps 4 lt 3 lc rgb "#FF345C" linewidth 10 


