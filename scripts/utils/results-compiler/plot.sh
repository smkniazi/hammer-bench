#!/bin/bash

#check for installation of parallel-rsync
if [ ! -e /usr/bin/pdftk ] ; then
echo "You do not appear to have installed: pdftk"
echo "sudo aptitude install pdftk"
exit
fi

if [ ! -e /usr/bin/pdfcrop ] ; then
echo "You do not appear to have installed: pdfcrop"
echo "sudo aptitude install pdfcrop"
exit
fi

if [ -z $GDFONTPATH ] ; then
export GDFONTPATH=/usr/share/fonts/truetype/msttcorefonts/
fi


sed '/hopsdelimeter.*/q' histo.gnuplot > truncated
cat truncated histo-internal.gnuplot > histo.gnuplot
rm truncated



gnuplot  histo.gnuplot
sleep 2
pdfcrop ./histogram.pdf

#rotate and crop
pdftk ./histogram-crop.pdf cat 1east output ./histogram.pdf  
rm ./histogram-crop.pdf 

#no rotation
#mv ./histogram-crop.pdf  ./histogram.pdf




gnuplot  interleaved.gnuplot
sleep 2
pdfcrop ./interleaved.pdf
mv ./interleaved-crop.pdf ./interleaved.pdf


gnuplot  block-report.gnuplot
sleep 2
pdfcrop ./block-report.pdf
mv ./block-report-crop.pdf ./block-report.pdf



