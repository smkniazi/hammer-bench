#!/bin/bash
# Author: Salman Niazi 2015
# Run all the damn benchmarks


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
OS=$($DIR/os-type.sh)
PSSH=
if [ $OS == "Ubuntu" ] ; then
   PSSH="/usr/bin/parallel-ssh"
elif [ $OS == "CentOS" ] ; then
   PSSH="/usr/bin/pssh"
fi

echo $PSSH

