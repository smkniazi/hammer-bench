#!/bin/bash

for i in `seq 0 31`; do 
       cat  /sys/class/net/eth0/queues/rx-$i/rps_cpus 
done
