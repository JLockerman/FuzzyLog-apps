#!/usr/bin/gnuplot
set print "-"
help_string="Usage: gnuplot -e \"file_in='<input path>'; file_out='<output path>'; row_start=<start row #>; row_end=<end row #>;\"";

if ((!exists("file_in")) || (!exists("file_out"))) \
	print "file_in or file_out undefined"; \
	print help_string; \
	exit;
 
if ((!exists("row_start")) || (!exists("row_end"))) \
	print "row_start or row_end undefined"; \
	print help_string;

set term postscript eps enhanced size 2.5in,2.5in font "Helvetica,11"

#set style data filledcurves 
#set style data histogram 
#set style data linespoints
set style data lines

# KEY (LEGEND)
set key
#set key right top
#set nokey
#set key box

# X LABEL AND TICS
set xlabel "Number of clients"
set xtics nomirror
set xrange[row_start:row_end]
#set xtics nomirror out rotate by -40 ("16KB" 0, "256KB" 4, "4MB" 8, "64MB" 12, "1GB" 16, "16GB" 20, "256GB" 24)

# Y LABEL AND TICS
set ylabel "Throughput (txns/sec)"
set ytics nomirror
set yrange[0:]
#set ytic("0" 0, "100" 100, "200" 200, "300" 300, "400" 400, "500" 500, "600" 600)
#set y2label "Ks of TXes/sec"
#unset y2label
#set y2range[0:25.6]
#set y2tics 5
#set y2tic("" 5, "" 10, "" 15, "" 20, "" 25)

# OUTPUT FILE
set output file_out

# ACTUAL PLOTTING
plot file_in every ::row_start::row_end using 1:($2) title ''

