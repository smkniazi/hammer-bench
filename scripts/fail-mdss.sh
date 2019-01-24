#!/bin/bash

for i in {0..$1}; 
do 
  sudo ceph mds fail $i; 
done 
