set terminal postscript eps enhanced color font "Helvetica,14"  #monochrome
set output '| ps2pdf - block-report.pdf'
plot 'block-report.dat' using 2:xticlabels(1) not with lines, '' using 0:2:3:4:xticlabels(1) title "HopsFS" with errorbars, 0.0 title "HDFS" 
