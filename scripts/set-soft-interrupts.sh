#!bin/bash

for i in `seq 0 31`; do 
       echo FC000000 > /sys/class/net/eth0/queues/rx-$i/rps_cpus 
done
