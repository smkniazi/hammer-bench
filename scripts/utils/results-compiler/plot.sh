#!/bin/bash
# Copyright (C) 2022 HopsWorks.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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



