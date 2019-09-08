#!/bin/bash
cd ..
tar zcf hopsbench.tgz hammer-bench
cp hopsbench.tgz ~/code/hops/hopsfs-cloud-dist/ 
rm hopsbench.tgz
