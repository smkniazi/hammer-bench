#!/bin/bash
#
#   Licensed to the Apache Software Foundation (ASF) under one or more
#   contributor license agreements.  See the NOTICE file distributed with
#   this work for additional information regarding copyright ownership.
#   The ASF licenses this file to You under the Apache License, Version 2.0
#   (the "License"); you may not use this file except in compliance with
#   the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# Author: Salman Niazi 2015



#Experiments
HopsFS_Src_Folder=../
HopsFS_Experiments_Remote_Dist_Folder=/tmp/hops-benchmark-jars
HopsFS_Rebuild_Exp_Src=false
HopsFS_Upload_Exp=true
CPU_AFFINITY=1-23

#Machines
DNS_FullList=(`grep -v "^#" datanodes`) 
NNS_FullList=(`grep -v "^#" namenodes`)

BM_Machines_FullList=(`grep -v "^#" experiment-nodes`)      #These are the machines that run the benchmark application. Basically, these machines are containers for DFSClients. 
DFS_CLIENTS_PER_NAMENODE=900                         #In RAW and INTERLEAVED benchmarks use DFS_CLIENTS_PER_NAMENODE*(No of active namenodes in the experiment) clients to stress the namenodes.
                                                   #These clients are uniformly distributed among the benchmark (BM_Machines_FullList) machines. 
                                                   #if DFS_CLIENTS_PER_NAMENODE=1000, 5 namenodes and two benchmark machines (BM_Machines_FullList) then each benchmark machine will have 2500 DFSClients
TINY_DATANODES_PER_NAMENODE=5                      #No of simulated datanodes for benchmarking the blockreporting system

#experiments to run
#NOTE all experiment related parameters are in master.properties file
Benchmark_Types=(
          #  RAW                                         #Test raw throughput of individual operations
            INTERLEAVED                                  #Test synthetic workload from spotify 
            #BR                                          #Block report testing. Set the hart beat time for the datanodes to Long.MAX_VALUE. We use a datanode class that does not send HBs  
            ) #space is delimeter

NN_INCREMENT=111
EXP_START_INDEX=1
REPEAT_EXP_TIMES=1


All_Results_Folder="/tmp/hops-bm/"                                        #This is where the results are saved. 
exp_remote_bench_mark_result_dir="/tmp/hops-bm-master-results/"           #This the folder on where the master sotres the results. 
NumberNdbDataNodes=4                                                      #added to the results of the benchmarks. helps in data aggregation. for HDFS set it to 0             

            


#HopsFS Distribution Parameters
HopsFS_User=nzo
NameNodeRpcPort=26801
HopsFS_Remote_Dist_Folder=/tmp/hopsfs
Datanode_Data_Dir=$HopsFS_Remote_Dist_Folder/Data
#full path to java
JAVA_BIN=java











                             

