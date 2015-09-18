set terminal postscript eps enhanced color font "Helvetica,18"  #monochrome
set output '| ps2pdf - interleaved.pdf'
#set size 1,0.75 
 set ylabel "ops/sec" 
set xlabel "Number of Namenodes" 
set format y "%.0s%c"
plot  'Spotify-interleaved.dat' using 2:xticlabels(1) not with lines, '' using 0:2:3:4:xticlabels(1) title "HopsFS-Spotify" with errorbars, 43854.67 title "HDFS-Spotify"  , \
