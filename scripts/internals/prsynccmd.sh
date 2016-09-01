#!/bin/bash
# Author: Salman Niazi 2015
# Run all the damn benchmarks


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
OS=$($DIR/os-type.sh)
PRSYNC=
if [ $OS == "Ubuntu" ] ; then
   PRSYNC="/usr/bin/parallel-rsync"
elif [ $OS == "CentOS" ] ; then
   PRSYNC="/usr/bin/prsync"
fi

echo $PRSYNC

