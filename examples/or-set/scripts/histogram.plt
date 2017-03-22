set terminal postscript eps color solid enhanced font "Helvetica"
set auto x
set output 'histogram.eps'
set style data histogram 
set style histogram cluster gap 1 
set boxwidth 0.9 
set xtic scale 0

plot 'server_stats.txt' every ::1::1 using 2:xticlabels(1) , \
     'server_stats.txt' every ::2::2 using 2::xticlabels(1), '' u 3,\
     'server_stats.txt' every ::3::3 using 2::xticlabels(1), '' u 3, '' u 4, \
     'server_stats.txt' every ::4::4 using 2::xticlabels(1), '' u 3, '' u 4, '' u 5
 
