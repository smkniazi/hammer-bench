#!/bin/bash
cd ..
tar zcf hopsbench.tgz hammer-bench
scp hopsbench.tgz glassfish@snurran.sics.se:/var/www/hops
rm hopsbench.tgz
cd HDFS-Distributed-BenchMark
