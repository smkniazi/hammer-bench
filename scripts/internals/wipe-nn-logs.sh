#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script



#load config parameters
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
PSSH=$(internals/psshcmd.sh)
PRSYNC=$(internals/prsynccmd.sh)

$PSSH -H "${NNS_FullList[*]}"  -l $HopsFS_User -i rm -rf  "$HopsFS_Remote_Dist_Folder/logs/*"






