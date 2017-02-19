#!/usr/bin/gnuplot
clear
reset
set terminal postscript eps color solid enh font "Helvetica"
# output
set output 'latency.eps'

# graph size, border, layout
set size 1.5,2.2
set border lw 3
set multiplot layout 2, 1


##### PLOT 1 #####
set origin 0,1.125

# size
set size 1.5,0.9

# title
set notitle
#set title "Latency histogram" font ",30" offset 0,5

# legend
#set key out
#set key at 14,330 horizontal
set nokey
#set key right top
#set key font ",24"
#set key width -8

# x-axis
set format x "%1.2f"
set xtics font ",24" rotate out
set xlabel 'Latency (ms)' font ",24" offset 0,-2

# y-axis
set ytics font ",24" 
#set autoscale y
set yrange [0:700]
#set format y "%3.0f K"

# style 
set style data histogram
set style histogram cluster gap 0
set style fill solid border -1
set boxwidth 0.9

# label
set label 1 "(a) 2K async appends (window size = 32)" at 4,790 font ",30"


# plot
plot for [COL=2:2] "latency_async.data" using COL:xticlabels(1) fillcolor rgb "#FF0000"




##### PLOT 2 #####
unset key
set notitle
set origin 0,0.05
set size 1.5,0.9
set yrange[0:2000]

# label
unset label 1
set label 1 "(b) 2K sync appends" at 6,2250 font ",30"

plot for [COL=2:2] "latency_sync.data" using COL:xticlabels(1) fillcolor rgb "#FF235C"
