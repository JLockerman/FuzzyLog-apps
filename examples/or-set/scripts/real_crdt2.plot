#!/usr/bin/gnuplot
set terminal pdf font "Times-Roman,9" color size 3.3,2.1
set output "real_crdt2.pdf"
set yrange [0:1.5]
set y2range [0:4]
set xrange [0:100]
set boxwidth 2
#set arrow from 1000,0 to 1000,1000 nohead lt 2
#set arrow from 3000,0 to 3000,1000 nohead lt 2
#set object 1 rect from 1000,0 to 3000,1000 fc rgb "black" fs solid 0.2
set xtics 10
set ytics nomirror
set ytics 0.5
set y2tics 1
set xlabel "Time Elapsed (secs)"
set ylabel "Throughput (Ms of puts/sec)"
set y2label "Staleness (Ms of missing puts)"
#set key top left
set xtics nomirror out rotate by -90 
plot "real_crdt2.in" using ($1):($3/1000) every 2 with boxes title "Staleness (right y-axis)" lt rgbcolor "black" fs solid 0.1 axes x1y2,\
"" using ($1):($2/1000) with lines title "CRDTMap puts/sec (left y-axis)" lt rgbcolor "black" lw 3
