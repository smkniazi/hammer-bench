#!/bin/bash

for (( i=0; i< $1; i++))
do
 sudo ceph mds fail $i
done 
