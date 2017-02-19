#!/usr/bin/gnuplot
clear
reset
set terminal postscript eps color solid enh font "Helvetica"

# graph size, border, origin
#set size 1.5,1.5
set border lw 3
set origin 0,1.025

# title
set title "Latency histogram" font ",30" offset 0,2

# legend
#set key out
#set key at 14,330 horizontal
#set nokey
set key right top
set key font ",24"
#set key width -8

# x-axis
set format x "%1.2f"
set xtics font ",24" rotate out
set xlabel 'Latency (ms)' font ",24" offset 0,-2

# y-axis
set ytics font ",24" 
#set autoscale y
set yrange [0:1000]
#set format y "%3.0f K"

# style 
set style data histogram
set style histogram cluster gap 0
set style fill solid border -1
set boxwidth 0.9


# output
set output 'latency.eps'

plot for [COL=2:2] "latency.data" using COL:xticlabels(1) title "2K async appends (window size = 32)"
