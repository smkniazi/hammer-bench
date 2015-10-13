#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $DIR/deployment.properties


if [ -z $1 ]; then
	echo "please, specify the download folder"
	exit
fi

rm -rf $1
mkdir -p $1

#All Unique Hosts
All_Hosts=${HOP_Default_NN[*]}" "${HOP_NN_List[*]}" "${HOP_DN_List[*]}" "${YARN_MASTERS[*]}
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')


for i in ${All_Unique_Hosts[@]}
do
	connectStr="$HopsFS_User@$i"
	ssh $connectStr 'rm  -f  '  $HopsFS_Remote_Dist_Folder/$i.tar.bz2
	ssh $connectStr 'tar -cf ' $HopsFS_Remote_Dist_Folder/$i.tar   $HopsFS_Remote_Dist_Folder/logs
	ssh $connectStr 'bzip2  '  $HopsFS_Remote_Dist_Folder/$i.tar
	
        folder=$1/$i 
        mkdir -p $folder
	scp $connectStr:$HopsFS_Remote_Dist_Folder/$i.tar.bz2 $folder
        ssh $connectStr 'rm  '  $HopsFS_Remote_Dist_Folder/$i.tar.bz2 
        bzip2 -d $1/$i/$i.tar.bz2
        tar -xf  $1/$i/$i.tar  -C $1/$i/ 
done





