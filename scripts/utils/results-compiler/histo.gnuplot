set terminal postscript eps enhanced color font "Helvetica,12"  #monochrome
set output '| ps2pdf - histogram.pdf'
set datafile missing '-'

set style fill solid  #noborder

unset key
unset ytics
set grid ytics 

set style histogram rowstacked title textcolor lt -1 font "bold,9" offset character 0, -1.2, 0
set style data histograms


#set label 10 at 3,160000 LABEL front center


set obj 1 rect at 3,155000 size 1,5000 fs solid 1 fc rgb "#1f78b4"
LABEL = "HDFS"
set label 1 at 3,165000 LABEL front center rotate by -270


set obj 2 rect at 4.5,145000 size 1,5000 fs solid 1 fc rgb "#d73027"
set obj 3 rect at 4.5,150000 size 1,5000 fs solid 1 fc rgb "#f46d43"
set obj 4 rect at 4.5,155000 size 1,5000 fs solid 1 fc rgb "#fdae61"
LABEL2 = "HopsFS"
set label 4 at 4.5,166500 LABEL2 front center rotate by -270




set xtics border in scale 0,0 nomirror rotate by -270  autojustify
set xtics  norangelimit font ",10" 
unset xtics

set ytics border in scale 0,0 mirror norotate  autojustify
set ytics autofreq  norangelimit font ",12" rotate by 90 offset character 0, 0, 0
set yrange [0:180000]
set format y "%.0s%c"
set ytics ("" 0, "20K" 20000,"40K" 40000, "60K" 60000,"80K" 80000,"100K" 100000,"120K" 120000, "140K" 140000, "160K" 160000, "180K" 180000, "200K" 200000)



set y2tics mirror autofreq  norangelimit font ",12" right rotate by 90 offset character -0.7, 0, 0
set y2tics ("" 0, "20K" 20000,"40K" 40000, "60K" 60000,"80K" 80000,"100K" 100000,"120K" 120000, "140K" 140000, "160K" 160000, "180K" 180000, "200K" 200000)
#set format y2 "%.0s%c"
set y2label "ops/sec" offset character -3, 0, 0

#hopsdelimeter. do not delete this line. The text below is imported by the plot.sh from the histo-internal.gnuplot file. Make the changes in the histo-internal.gnuplot file if you want to change the following lines. 
plot  newhistogram "MKDIR", 'MKDIR.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" t col lc rgb '#d73027'  , '' u "2-NN" t col lc rgb '#f46d43'  , '' u "3-NN" t col lc rgb '#fdae61'  , '' u "4-NN" t col lc rgb '#fee08b'  , '' u "5-NN" t col lc rgb '#d73027'  , '' u "6-NN" t col lc rgb '#f46d43'  , '' u "7-NN" t col lc rgb '#fdae61'  , '' u "8-NN" t col lc rgb '#fee08b'  , '' u "9-NN" t col lc rgb '#d73027'  , '' u "10-NN" t col lc rgb '#f46d43'  , '' u "11-NN" t col lc rgb '#fdae61'  , '' u "12-NN" t col lc rgb '#fee08b'  , \
 newhistogram "CREATE\nFILE", 'CREATE_FILE.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "APPEND\nFILE", 'APPEND_FILE.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "READ\nFILE", 'READ_FILE.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "LS\nDIR", 'LS_DIR.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "LS\nFILE", 'LS_FILE.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "CHMOD\nFILE", 'CHMOD_FILE.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "CHMOD\nDIR", 'CHMOD_DIR.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "INFO\nFILE", 'INFO_FILE.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "INFO\nDIR", 'INFO_DIR.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "SET\nREPL", 'SET_REPL.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "RENAME\nFILE", 'RENAME_FILE.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "DEL\nFILE", 'DEL_FILE.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "CHOWN\nFILE", 'CHOWN_FILE.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
 newhistogram "CHOWN\nDIR", 'CHOWN_DIR.dat'  using "SingleNN":xtic(1) not  lc rgb '#1f78b4', '' u "1-NN" not lc rgb '#d73027'  , '' u "2-NN" not lc rgb '#f46d43'  , '' u "3-NN" not lc rgb '#fdae61'  , '' u "4-NN" not lc rgb '#fee08b'  , '' u "5-NN" not lc rgb '#d73027'  , '' u "6-NN" not lc rgb '#f46d43'  , '' u "7-NN" not lc rgb '#fdae61'  , '' u "8-NN" not lc rgb '#fee08b'  , '' u "9-NN" not lc rgb '#d73027'  , '' u "10-NN" not lc rgb '#f46d43'  , '' u "11-NN" not lc rgb '#fdae61'  , '' u "12-NN" not lc rgb '#fee08b'  , \
