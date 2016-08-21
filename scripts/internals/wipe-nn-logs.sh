#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script



#load config parameters
PSSH=
PRSYNC=
OS=$(./os-type.sh)
if [ $OS == "Ubuntu" ] ; then
   PRSYNC="/usr/bin/parallel-rsync"
   PSSH="/usr/bin/parallel-ssh"
elif [ $OS == "CentOS" ] ; then
   PRSYNC="/usr/bin/prsync"
   PSSH="/usr/bin/pssh"
fi

$PSSH -H "${NNS_FullList[*]}"  -l $HopsFS_User -i rm -rf  "$HopsFS_Remote_Dist_Folder/logs/*"






