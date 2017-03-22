set terminal postscript eps color solid enhanced font 'Helvetica' 
set output 'histogram.eps'
set title "Packets received per server"
C = "#99ffff"; Cpp = "#4671d5"; Java = "#ff0000"; Python = "#f36e00"
set auto x
set logscale y
set yrange [1:10000000]
set style data histograms
set xlabel '# Fuzzylog Servers' 
set ylabel '# Received packets'
set style histogram cluster gap 1 
set style fill solid border -1
set boxwidth 0.9 
set xtic scale 0
# 2, 3, 4, 5 are the indexes of the columns; 'fc' stands for 'fillcolor'
plot 'server_stats.txt' using 2:xtic(1) ti col fc rgb C, '' u 3 ti col fc rgb Cpp, '' u 4 ti col fc rgb Java, '' u 5 ti col fc rgb Python, '' u 6 ti col fc rgb '#00FF9C'
