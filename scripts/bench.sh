#!/bin/bash
# Copyright (C) 2022 HopsWorks.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

Dist_Dir=.
Bench_JAR=hammer-bench.jar
HopsFS_Experiments_Remote_Dist_Folder=/tmp/hops-benchmark-jars
HopsFS_User=hdfs
NameNodeRpcPort=8020
All_Results_Folder="/tmp/hops-bm/"                                        #This is where the results are saved. 
exp_remote_bench_mark_result_dir="/tmp/hops-bm-master-results/"           #This the folder on where the master sotres the results. 
#full path to java or just java if PATH is set
JAVA_BIN=java
TOTAL_CLIENTS=40 
REPEAT_EXP_TIMES=1

#Machines
DNS_FullList=(`grep -v "^#" $DIR/datanodes`) 
NNS_FullList=(`grep -v "^#" $DIR/namenodes`)

#These are the machines that run the benchmark application.
#Basically, these machines are containers for DFSClients. 
BM_Machines_FullList=$($DIR/experiment-nodes.sh)      

#experiments to run
#NOTE all experiment related parameters are in master.properties file
Benchmark_Types=(
  #RAW #Test raw throughput of individual operations
  INTERLEAVED #Test synthetic workload from spotify 
  ) 

exp_master_prop_file="$DIR/master.properties"
exp_deploy_script="$DIR/internals/upload_experiments.sh"
exp_start_script="$DIR/internals/start-exp.sh"
exp_stop_script="$DIR/internals/kill-exp.sh"

#############################################################################################################################
run() {
  echo "*************************** Exp Params Start ****************************"
  echo "All_NNs_In_Current_Exp: ${All_NNs_In_Current_Exp[@]}"
  echo "Current_Leader_NN $Current_Leader_NN"
  echo "Non_Leader_NNs $Non_Leader_NNs"
  echo "Slaves $ExpMaster ${ExpSlaves[@]}"
  echo "Master $ExpMaster"
  echo "Threads/Slave $ClientsPerSlave"
  echo "currentExpDir $currentExpDir"
  echo "BenchMark $BenchMark"
  echo "DataNodes $DNS_FullList_STR"
  echo "Bootstrap NN $BOOT_STRAP_NN"
  echo "*************************** Exp Params End ****************************"

  sed -i 's|list.of.slaves.*|list.of.slaves='"$ExpMaster ${ExpSlaves[@]}"'|g'       $exp_master_prop_file
  sed -i 's|benchmark.type.*|benchmark.type='$BenchMark'|g'                         $exp_master_prop_file
  sed -i 's|num.slave.threads.*|num.slave.threads='$ClientsPerSlave'|g'             $exp_master_prop_file
  sed -i 's|results.dir.*|results.dir='$exp_remote_bench_mark_result_dir'|g'        $exp_master_prop_file      
  sed -i 's|fs.defaultFS=.*|fs.defaultFS='$BOOT_STRAP_NN'|g'                        $exp_master_prop_file
  sed -i 's|no.of.namenodes.*|no.of.namenodes='$TotalNNCount'|g'                    $exp_master_prop_file

  echo "*** strating the benchmark ***"
  date1=$(date +"%s") 
  ssh $HopsFS_User@$ExpMaster mkdir -p $exp_remote_bench_mark_result_dir
  source $exp_start_script $ExpMaster 
  scp $HopsFS_User@$ExpMaster:$exp_remote_bench_mark_result_dir/* $currentExpDir/

  echo "*** shutting down the exp nodes ***" 
  source $exp_stop_script           # kills exp
  date2=$(date +"%s")
  diff=$(($date2-$date1))
  cat $currentExpDir/*.txt
  echo "ExpTime $currentExpDir $(($diff / 60)) minutes and $(($diff % 60)) seconds."
}

rm -rf $All_Results_Folder
mkdir -p $All_Results_Folder
counter=0

# deploying experiment jars
source $exp_deploy_script

while [  $counter -lt $REPEAT_EXP_TIMES ]; do
  let counter+=1
  currentDir="$All_Results_Folder/run_$counter"
  mkdir -p $currentDir

  TotalNNCount=${#NNS_FullList[@]}
  currentNNIndex=0
  Current_Leader_NN=""
  Non_Leader_NNs=""
  All_NNs_In_Current_Exp=""
  for ((e_i = 0; e_i < $TotalNNCount; e_i++)) do
    if [ $e_i -eq 0 ]; then
      Current_Leader_NN=${NNS_FullList[$e_i]}
    else
      Non_Leader_NNs="$Non_Leader_NNs ${NNS_FullList[$e_i]}"
    fi
  done
  All_NNs_In_Current_Exp="$All_NNs_In_Current_Exp $Current_Leader_NN"

  for ((e_x = 0; e_x < ${#Benchmark_Types[@]}; e_x++)) do
    BenchMark=${Benchmark_Types[$e_x]}

    DNS_FullList_STR=""
    for ((e_dn = 0; e_dn < ${#DNS_FullList[@]}; e_dn++)) do
      DNS_FullList_STR="$DNS_FullList_STR ${DNS_FullList[$e_dn]}"
    done

    currentDirBM="$currentDir/$BenchMark"
    mkdir -p $currentDirBM

    TotalSlaves=${#BM_Machines_FullList[@]}
    ClientsPerSlave=$(echo "scale=2; ($TOTAL_CLIENTS)/$TotalSlaves" | bc)

    #ceiling
    ClientsPerSlave=$(echo "scale=2; ($ClientsPerSlave + 0.5) " | bc)
    ClientsPerSlave=$(echo "($ClientsPerSlave/1)" | bc)
    TotalClients=$(echo "($ClientsPerSlave * $TotalSlaves)" | bc) #recalculate

    ExpSlaves=""
    ExpMaster=""
    for ((e_k = 0; e_k < ${#BM_Machines_FullList[@]}; e_k++)) do
      if [ -z "$ExpMaster" ]; then
        ExpMaster=${BM_Machines_FullList[$e_k]}
      else
        ExpSlaves="$ExpSlaves ${BM_Machines_FullList[$e_k]}"
      fi
    done

    if [ -z "$NameNodeRpcPort" ]; then
      BOOT_STRAP_NN="hdfs://$Current_Leader_NN"                                         
    else
      RPC_PORT=$(echo "($NameNodeRpcPort)" | bc)
      BOOT_STRAP_NN="hdfs://$Current_Leader_NN:$RPC_PORT"                   
    fi

    currentExpDir="$currentDirBM/$TotalClients-Clients-$BenchMark-BenchMark"
    mkdir -p  $currentExpDir       
    run
  done
done
exit


