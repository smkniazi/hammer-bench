/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.experiments.benchmarks.common.config;

/**
 *
 * @author salman
 */
public class ConfigKeys {
    
    public static final String BENCHMARK_FILE_SYSTEM_NAME_KEY = "benchmark.filesystem.name";
    public static final String BENCHMARK_FILE_SYSTEM_NAME_DEFAULT = "HDFS";
  
    public static final int BUFFER_SIZE = 4*1024*1024; 
    
    public static final String RAW_RESPONSE_FILE_EXT = ".responses";
    
    public static String MAX_SLAVE_FAILURE_THREASHOLD_KEY = "max.slave.failure.threshold";   // only for logging
    public static int MAX_SLAVE_FAILURE_THREASHOLD_DEFAULT = 0;
    
    public static String NO_OF_NAMENODES_KEY = "no.of.namenodes";   // only for logging
    public static int NO_OF_NAMENODES_DEFAULT = 1;
    
    public static String NO_OF_NDB_DATANODES_KEY = "no.of.ndb.datanodes";   // only for logging
    public static int NO_OF_NDB_DATANODES_DEFAULT = 0;
    
    public static String BENCHMARK_TYPE_KEY = "benchmark.type";
    public static String BENCHMARK_TYPE_DEFAULT = "RAW";// "Type. RAW | INTERLEAVED | BM ."
    
    public static String GENERATE_PERCENTILES_KEY = "generate.percentiles";
    public static boolean   GENERATE_PERCENTILES_DEFAULT = true;
    
    public static String INTERLEAVED_BM_DURATION_KEY = "interleaved.bm.duration";
    public static long   INTERLEAVED_BM_DURATION_DEFAULT = 60*1000;
    
    public static String RAW_CREATE_PHASE_MAX_FILES_TO_CRAETE_KEY = "raw.create.phase.max.files.to.create";
    public static long RAW_CREATE_PHASE_MAX_FILES_TO_CRAETE_DEFAULT = Long.MAX_VALUE;
    
    public static String RAW_CREATE_FILES_PHASE_DURATION_KEY = "raw.create.files.phase.duration";
    public static long    RAW_CREATE_FILES_PHASE_DURATION_DEFAULT = 0; 
    
    public static String INTLVD_CREATE_FILES_PERCENTAGE_KEY = "interleaved.create.files.percentage";
    public static double    INTLVD_CREATE_FILES_PERCENTAGE_DEFAULT = 0; 
    
    public static String RAW_READ_FILES_PHASE_DURATION_KEY = "raw.read.files.phase.duration"; 
    public static long   RAW_READ_FILES_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_READ_FILES_PERCENTAGE_KEY = "interleaved.read.files.percentage";
    public static double    INTLVD_READ_FILES_PERCENTAGE_DEFAULT = 0; 
     
    public static String RAW_RENAME_FILES_PHASE_DURATION_KEY = "raw.rename.files.phase.duration"; 
    public static long   RAW_RENAME_FILES_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_RENAME_FILES_PERCENTAGE_KEY = "interleaved.rename.files.percentage";
    public static double    INTLVD_RENAME_FILES_PERCENTAGE_DEFAULT = 0; 
     
    public static String RAW_LS_FILE_PHASE_DURATION_KEY = "raw.ls.files.phase.duration"; 
    public static long   RAW_LS_FILE_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_LS_FILE_PERCENTAGE_KEY = "interleaved.ls.files.percentage";
    public static double    INTLVD_LS_FILE_PERCENTAGE_DEFAULT = 0; 
    
    public static String RAW_LS_DIR_PHASE_DURATION_KEY = "raw.ls.dirs.phase.duration"; 
    public static long   RAW_LS_DIR_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTERLEAVED_WORKLOAD_NAME_KEY = "interleaved.workload.name";
    public static String INTERLEAVED_WORKLOAD_NAME_DEFAULT = "default"; 
    
    public static String INTLVD_LS_DIR_PERCENTAGE_KEY = "interleaved.ls.dirs.percentage";
    public static double    INTLVD_LS_DIR_PERCENTAGE_DEFAULT = 0; 
    
    public static String RAW_DElETE_FILES_PHASE_DURATION_KEY = "raw.delete.files.phase.duration"; 
    public static long   RAW_DELETE_FILES_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_DELETE_FILES_PERCENTAGE_KEY = "interleaved.delete.files.percentage";
    public static double    INTLVD_DELETE_FILES_PERCENTAGE_DEFAULT = 0; 
    
    public static String RAW_CHMOD_FILES_PHASE_DURATION_KEY = "raw.chmod.files.phase.duration"; 
    public static long   RAW_CHMOD_FILES_PHASE_DURATION_DEFAULT = 0;
    
    public static String RAW_CHMOD_DIRS_PHASE_DURATION_KEY = "raw.chmod.dirs.phase.duration"; 
    public static long   RAW_CHMOD_DIRS_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_CHMOD_FILES_PERCENTAGE_KEY = "interleaved.chmod.files.percentage";
    public static double    INTLVD_CHMOD_FILES_PERCENTAGE_DEFAULT = 0; 
    
    public static String INTLVD_CHMOD_DIRS_PERCENTAGE_KEY = "interleaved.chmod.dirs.percentage";
    public static double    INTLVD_CHMOD_DIRS_PERCENTAGE_DEFAULT = 0; 
    
    public static String RAW_MKDIR_PHASE_DURATION_KEY = "raw.mkdir.phase.duration"; 
    public static long   RAW_MKDIR_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_MKDIR_PERCENTAGE_KEY = "interleaved.mkdir.percentage";
    public static double    INTLVD_MKDIR_PERCENTAGE_DEFAULT = 0; 

    public static String RAW_SETREPLICATION_PHASE_DURATION_KEY = "raw.file.setReplication.phase.duration"; 
    public static long   RAW_SETREPLICATION_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_SETREPLICATION_PERCENTAGE_KEY = "interleaved.file.setReplication.percentage";
    public static double    INTLVD_SETREPLICATION_PERCENTAGE_DEFAULT = 0;     
    
    public static String RAW_GET_FILE_INFO_PHASE_DURATION_KEY = "raw.file.getInfo.phase.duration"; 
    public static long   RAW_GET_FILE_INFO_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_GET_FILE_INFO_PERCENTAGE_KEY = "interleaved.file.getInfo.percentage";
    public static double    INTLVD_GET_FILE_INFO_PERCENTAGE_DEFAULT = 0;     
    
    public static String RAW_GET_DIR_INFO_PHASE_DURATION_KEY = "raw.dir.getInfo.phase.duration"; 
    public static long   RAW_GET_DIR_INFO_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_GET_DIR_INFO_PERCENTAGE_KEY = "interleaved.dir.getInfo.percentage";
    public static double    INTLVD_GET_DIR_INFO_PERCENTAGE_DEFAULT = 0;     
    
    public static String RAW_FILE_APPEND_PHASE_DURATION_KEY = "raw.file.append.phase.duration"; 
    public static long   RAW_FILE_APPEND_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_APPEND_FILE_PERCENTAGE_KEY = "interleaved.file.append.percentage";
    public static double INTLVD_APPEND_FILE_PERCENTAGE_DEFAULT = 0;     
    
    public static String RAW_FILE_CHANGE_USER_PHASE_DURATION_KEY = "raw.file.change.user.phase.duration"; 
    public static long   RAW_FILE_CHANGE_USER_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_FILE_CHANGE_USER_PERCENTAGE_KEY = "interleaved.file.change.user.percentage";
    public static double INTLVD_FILE_CHANGE_USER_PERCENTAGE_DEFAULT = 0;     
    
    public static String RAW_DIR_CHANGE_USER_PHASE_DURATION_KEY = "raw.dir.change.user.phase.duration"; 
    public static long   RAW_DIR_CHANGE_USER_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_DIR_CHANGE_USER_PERCENTAGE_KEY = "interleaved.dir.change.user.percentage";
    public static double INTLVD_DIR_CHANGE_USER_PERCENTAGE_DEFAULT = 0;     
        
    public static String FS_CEPH_IMPL_KEY = "fs.ceph.impl";
    public static String FS_CEPH_IMPL_DEFAULT = "org.apache.hadoop.fs.ceph.CephFileSystem";
    
    public static String CEPH_AUTH_KEYRING_KEY = "ceph.auth.keyring";
    public static String CEPH_AUTH_KEYRING_DEFAULT = "/etc/ceph/ceph.client.admin.keyring";
    
    public static String CEPH_CONF_FILE_KEY = "ceph.conf.file";
    public static String CEPH_CONF_FILE_DEFAULT = "/etc/ceph/ceph.conf";
    
    public static String CEPH_ROOT_DIR_KEY = "ceph.root.dir";
    public static String CEPH_ROOT_DIR_DEFAULT = "/";
            
    public static String CEPH_MON_ADDRESS_KEY = "ceph.mon.address";
    public static String CEPH_MON_ADDRESS_DEFAULT = "machine:6789";
    
    public static String CEPH_AUTH_ID_KEY = "ceph.auth.id";
    public static String CEPH_AUTH_ID_DEFAULT = "user";
    
    public static String BR_BENCHMARK_DURATION_KEY = "br.benchmark.duration";
    public static int BR_BENCHMARK_DURATION_DEFAULT = 0;

    public static String BR_NUM_BLOCKS_PER_REPORT = "br.blocks.per.report";
    public static int BR_NUM_BLOCKS_PER_REPORT_DEFAULT = 10;

    public static String BR_NUM_BLOCKS_PER_FILE = "br.blocks.per.file";
    public static int BR_NUM_BLOCKS_PER_FILE_DEFAULT = 10;

    public static String BR_NUM_FILES_PER_DIR = "br.files.per.dir";
    public static int BR_NUM_FILES_PER_DIR_DEFAULT = 10;

    public static String BR_MAX_BLOCK_SIZE= "br.max.block.size";
    public static int BR_MAX_BLOCK_SIZE_DEFAULT = 16;

    public static String BR_SKIP_CREATIONS = "br.skip.creations";
    public static boolean BR_SKIP_CREATIONS_DEFAULT = false;

    public static String BR_MAX_TIME_BEFORE_NEXT_REPORT =
        "br.max.time.before.nextreport";
    public static int BR_MAX_TIME_BEFORE_NEXT_REPORT_DEFAULT = 5000;

    public static String BR_MIN_TIME_BEFORE_NEXT_REPORT =
        "br.min.time.before.nextreport";
    public static int BR_MIN_TIME_BEFORE_NEXT_REPORT_DEFAULT = 1000;

    public static String BR_PERSIST_DATABASE = "br.persist.database";
    public static String BR_PERSIST_DATABASE_DEFAULT = "example.com:3306";

    public static String REPLICATION_FACTOR_KEY = "replication.factor";
    public static short  REPLICATION_FACTOR_DEFAULT = 3;

    //format list of tuples
    //[(size,percentage),(size,percentage)]
    //[(1024,10),(2048,90)]
    //all percentages should add to 100
    public static String FILE_SIZE_IN_Bytes_KEY= "file.size";
    public static String FILE_SIZE_IN_Bytes_DEFAULT = "[(0,100)]";
    
    public static String APPEND_FILE_SIZE_IN_Bytes_KEY= "append.size";
    public static long   APPEND_FILE_SIZE_IN_Bytes_DEFAULT = 0;

    public static String READ_FILES_FROM_DISK= "read.files.from.disk";
    public static boolean READ_FILES_FROM_DISK_DEFAULT=false;

    public static String DISK_FILES_PATH="disk.files.path";
    public static String DISK_FILES_PATH_DEFAULT="~";
    
    public static String DIR_PER_DIR_KEY= "dir.per.dir";
    public static int    DIR_PER_DIR_DEFAULT = 2;
    
    public static String FILES_PER_DIR_KEY= "files.per.dir";
    public static int    FILES_PER_DIR_DEFAULT = 16;
    
    public static String  ENABLE_FIXED_DEPTH_TREE_KEY = "enable.fixed.depth.tree";
    public static boolean ENABLE_FIXED_DEPTH_TREE_DEFAULT = false;
    
    public static String  TREE_DEPTH_KEY = "tree.depth";
    public static int     TREE_DEPTH_DEFAULT = 3;

    public static String NUM_SLAVE_THREADS_KEY = "num.slave.threads";
    public static int    NUM_SLAVE_THREADS_DEFAULT = 1;
      
    public static String BASE_DIR_KEY = "base.dir";
    public static String BASE_DIR_DEFAULT = "/test";

    public static String  SKIP_ALL_PROMPT_KEY = "skip.all.prompt";
    public static boolean SKIP_ALL_PROMPT_DEFAULT = false;
    
    public static String  ENABLE_REMOTE_LOGGING_KEY = "enable.remote.logging";
    public static boolean ENABLE_REMOTE_LOGGING_DEFAULT = true;
    
    public static String REMOTE_LOGGING_PORT_KEY = "remote.logging.port";
    public static int    REMOTE_LOGGING_PORT_DEFAULT = 6666;
    
    public static String SLAVE_LISTENING_PORT_KEY = "slave.listening.port";
    public static int    SLAVE_LISTENING_PORT_DEFAULT = 5555;
    
    public static String MASTER_LISTENING_PORT_KEY = "master.listening.port";
    public static int    MASTER_LISTENING_PORT_DEFAULT = 4444;
    
    public static String RESULTS_DIR_KEY = "results.dir";
    public static String RESULTS_DIR_DEFAULT =     ".";
    public static String TEXT_RESULT_FILE_NAME =   "hopsresults.txt";
    public static String BINARY_RESULT_FILE_NAME = "hopsresults.hopsbin";
    
    public static String FILES_TO_CRAETE_IN_WARM_UP_PHASE_KEY = "files.to.create.in.warmup.phase";
    public static int    FILES_TO_CRAETE_IN_WARM_UP_PHASE_DEFAULT = 10;
    
    public static String WARM_UP_PHASE_WAIT_TIME_KEY = "warmup.phase.wait.time";
    public static int    WARM_UP_PHASE_WAIT_TIME_DEFAULT = 1 * 60 * 1000;
    
    public static String LIST_OF_SLAVES_KEY = "list.of.slaves";
    public static String LIST_OF_SLAVES_DEFAULT = "localhost";
    
    public static String FS_DEFAULTFS_KEY = "fs.defaultFS";
    public static String FS_DEFAULTFS_DEFAULT = "";

    public static String DFS_NAMESERVICES = "dfs.nameservices";
    public static String DFS_NAMESERVICES_DEFAULT = "mycluster";

    public static String DFS_NAMENODE_SELECTOR_POLICY_KEY="dfs.namenode.selector-policy";
    public static String DFS_NAMENODE_SELECTOR_POLICY_DEFAULT="RANDOM_STICKY";
    
    public static String DFS_CLIENT_REFRESH_NAMENODE_LIST_KEY="dfs.client.refresh.namenode.list";
    public static long   DFS_CLIENT_REFRESH_NAMENODE_LIST_DEFAULT=60*60*1000;

    public static String DFS_CLIENT_MAX_RETRIES_ON_FAILURE_KEY="dfs.clinet.max.retires.on.failure";
    public static int   DFS_CLIENT_MAX_RETRIES_ON_FAILURE_DEFAULT=5;

    public static String DFS_CLIENT_INITIAL_WAIT_ON_FAILURE_KEY="dfs.client.initial.wait.on.retry";
    public static long   DFS_CLIENT_INITIAL_WAIT_ON_FAILURE_DEFAULT=0;

    public static String DFS_STORE_SMALL_FILES_IN_DB =  "dfs.store.small.files.in.db";
    public static final boolean DFS_STORE_SMALL_FILES_IN_DB_DEFAULT = false;

    public static final String DFS_DB_FILE_MAX_SIZE_KEY = "dfs.db.file.max.size";
    public static final int DFS_DB_FILE_MAX_SIZE_DEFAULT = 32*1024; // 32KB

    //failover test
    public static String TEST_FAILOVER= "test.failover";
    public static boolean TEST_FAILOVER_DEFAULT = false;

    public static String RESTART_NAMENODE_AFTER_KEY = "restart.a.namenode.after";
    public static long RESTART_NAMENODE_AFTER_DEFAULT = Long.MAX_VALUE;


    public static String FAIL_OVER_TEST_START_TIME_KEY = "failover.test.start.time";
    public static long FAIL_OVER_TEST_START_TIME_DEFAULT = Long.MAX_VALUE;

    public static String FAIL_OVER_TEST_DURATION_KEY = "failover.test.duration";
    public static long FAIL_OVER_TEST_DURATION_DEFAULT = Long.MAX_VALUE;

    public static String FAILOVER_NAMENODES= "failover.namenodes";
    public static String FAILOVER_NAMENODES_DEFAULT = null;

    public static String HADOOP_SBIN= "hadoop.sbin";
    public static String HADOOP_SBIN_DEFAULT = null;

    public static String HADOOP_USER= "hadoop.user";
    public static String HADOOP_USER_DEFAULT = null;

    public static String NAMENOE_RESTART_COMMANDS= "failover.nn.restart.commands";
    public static String NAMENOE_RESTART_COMMANDS_DEFAULT = null;

    public static String NAMENOE_KILLER_HOST_KEY= "namenode.killer";
    public static String NAMENOE_KILLER_HOST_DEFAULT = null;

    public static final String MASTER_SLAVE_WARMUP_DELAY_KEY= "master.slave.warmup.delay";
    public static final int MASTER_SLAVE_WARMUP_DELAY_KEY_DEFAULT = 0;
}
