#!/bin/bash
cd ..
tar zcf hopsbench.tgz HDFS-Distributed-BenchMark
scp hopsbench.tgz glassfish@snurran.sics.se:/var/www/hops
rm hopsbench.tgz
cd HDFS-Distributed-BenchMark
