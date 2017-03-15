set terminal postscript eps color solid enhanced font "Helvetica"
set output 'windowing.eps'

set size 1.5,2.3
set border lw 3
set multiplot layout 2,1

set key center center 

set label 1 "CRDT throughput and latency. Co-located single client and server" at 1,230 font ",32"
set key font ",32"
set origin 0,1.2
set size 1.5,1
set xtics font ",40" offset 0,-1 scale 4 
set ytics font ",40" offset -1 scale 4
set format y "%2.0f K"
set format x "%2.0f"
set ylabel 'Throughput ' font ",40" offset -9
set xrange [1:32]
set logscale x
set xtics ("1" 1, "2" 2, "4" 4, "8" 8, "16" 16, "32" 32)
set logscale y
set yrange [1:128]
set xlabel 'Window size' font ",40" offset 0,-2.5
plot "throughput_1.txt" using 1:2 title "1 client" with linespoints pt 6 ps 4 lt 4 lc rgb "red" linewidth 10, \
 "throughput_4.txt" using 1:2 title "1 client" with linespoints pt 6 ps 4 lt 4 lc rgb "orange" linewidth 10, \
"throughput_8.txt" using 1:2 title "1 client" with linespoints pt 6 ps 4 lt 4 lc rgb "dark-green" linewidth 10, \
 "throughput_12.txt" using 1:2 title "1 client" with linespoints pt 6 ps 4 lt 4 lc rgb "blue" linewidth 10, \
 "throughput_16.txt" using 1:2 title "1 client" with linespoints pt 6 ps 4 lt 4 lc rgb "dark-pink" linewidth 10 

unset xtics
unset key
set key bottom right
set origin 0,0
set size 1.5,1
set yrange [0:1.2]
set xrange [1:1000]
set format x "10^{%L}"
set format y " %2.1f "
set xtics font ",40" offset 0,-1 scale 4
set ytics font ",40" offset -1 scale 4
set ylabel 'Fraction of Requests' font ",40" offset -9
set xlabel 'Latency ({/Symbol m}s)' font ",40" offset 0,-2.5

set key font ",32"
set key maxcols 1
set key spacing 6
unset label 1

plot "c1_w1_latency.txt" using 2:1 title "Window sz 1" with lines lt 4 lc rgb "red" linewidth 10, \
 "c1_w2_latency.txt" using 2:1 title "Window sz 2" with lines lt 3 lc rgb "orange" linewidth 10, \
 "c1_w8_latency.txt" using 2:1 title "Window sz 8" with lines lt 8 lc rgb "dark-green" linewidth 10, \
 "c1_w16_latency.txt" using 2:1 title "Window sz 16" with lines lt 6 lc rgb "blue" linewidth 10, \
 "c1_w32_latency.txt" using 2:1 title "Window sz 32" with lines lt 5 lc rgb "dark-pink" linewidth 10





