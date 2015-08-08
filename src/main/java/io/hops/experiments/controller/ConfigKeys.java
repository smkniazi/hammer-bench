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
package io.hops.experiments.controller;

/**
 *
 * @author salman
 */
public class ConfigKeys {
  
    public static final int BUFFER_SIZE = 4096;
    
    public static String NO_OF_NAMENODES_KEY = "no.of.namenodes";
    public static int NO_OF_NAMENODES_DEFAULT = 1;
    
    public static String BENCHMARK_TYPE_KEY = "benchmark.type";
    public static String BENCHMARK_TYPE_DEFAULT = "RAW";// "Type. RAW | INTERLEAVED."
    
    public static String INTERLEAVED_BM_DURATION_KEY = "interleaved.bm.duration";
    public static long   INTERLEAVED_BM_DURATION_DEFAULT = 60*1000;
    
    public static String RAW_CREATE_PHASE_MAX_FILES_TO_CRAETE_KEY = "raw.create.phase.max.files.to.create";
    public static long RAW_CREATE_PHASE_MAX_FILES_TO_CRAETE_DEFAULT = Long.MAX_VALUE;
    
    public static String RAW_CREATE_FILES_PHASE_DURATION_KEY = "raw.create.files.phase.duration";
    public static long    RAW_CREATE_FILES_PHASE_DURATION_DEFAULT = 0; 
    
    public static String INTLVD_CREATE_FILES_PERCENTAGE_KEY = "interleaved.create.files.percentage";
    public static int    INTLVD_CREATE_FILES_PERCENTAGE_DEFAULT = 0; 
    
    public static String RAW_READ_FILES_PHASE_DURATION_KEY = "raw.read.files.phase.duration"; 
    public static long   RAW_READ_FILES_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_READ_FILES_PERCENTAGE_KEY = "interleaved.read.files.percentage";
    public static int    INTLVD_READ_FILES_PERCENTAGE_DEFAULT = 0; 
     
    public static String RAW_RENAME_FILES_PHASE_DURATION_KEY = "raw.rename.files.phase.duration"; 
    public static long   RAW_RENAME_FILES_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_RENAME_FILES_PERCENTAGE_KEY = "interleaved.rename.files.percentage";
    public static int    INTLVD_RENAME_FILES_PERCENTAGE_DEFAULT = 0; 
     
    public static String RAW_STAT_FILE_PHASE_DURATION_KEY = "raw.stat.files.phase.duration"; 
    public static long   RAW_STAT_FILE_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_STAT_FILE_PERCENTAGE_KEY = "interleaved.stat.files.percentage";
    public static int    INTLVD_STAT_FILE_PERCENTAGE_DEFAULT = 0; 
    
    public static String RAW_STAT_DIR_PHASE_DURATION_KEY = "raw.stat.dirs.phase.duration"; 
    public static long   RAW_STAT_DIR_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_STAT_DIR_PERCENTAGE_KEY = "interleaved.stat.dirs.percentage";
    public static int    INTLVD_STAT_DIR_PERCENTAGE_DEFAULT = 0; 
    
    public static String RAW_DElETE_FILES_PHASE_DURATION_KEY = "raw.delete.files.phase.duration"; 
    public static long   RAW_DELETE_FILES_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_DELETE_FILES_PERCENTAGE_KEY = "interleaved.delete.files.percentage";
    public static int    INTLVD_DELETE_FILES_PERCENTAGE_DEFAULT = 0; 
    
    public static String RAW_CHMOD_FILES_PHASE_DURATION_KEY = "raw.chmod.files.phase.duration"; 
    public static long   RAW_CHMOD_FILES_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_CHMOD_FILES_PERCENTAGE_KEY = "interleaved.chmod.files.percentage";
    public static int    INTLVD_CHMOD_FILES_PERCENTAGE_DEFAULT = 0; 
    
    public static String RAW_MKDIR_PHASE_DURATION_KEY = "raw.mkdir.phase.duration"; 
    public static long   RAW_MKDIR_PHASE_DURATION_DEFAULT = 0;
    
    public static String INTLVD_MKDIR_PERCENTAGE_KEY = "interleaved.mkdir.percentage";
    public static int    INTLVD_MKDIR_PERCENTAGE_DEFAULT = 0; 

    public static String BR_NUM_REPORTS = "br.numofreports";
    public static int BR_NUM_REPORTS_DEFAULT = 10;

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
    public static String BR_PERSIST_DATABASE_DEFAULT = "cloud1.sics" +
        ".se:3307:hop_mahmoud_test";

    public static String REPLICATION_FACTOR_KEY = "replication.factor";
    public static short  REPLICATION_FACTOR_DEFAULT = 3;
    
    public static String FILE_SIZE_IN_Bytes_KEY= "file.size";
    public static long   FILE_SIZE_IN_Bytes_DEFAULT = 0;

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
    
    public static String RESULTS_FIlE_KEY = "results.file";
    public static String RESULTS_FIlE_DEFAULT = "results.txt";
    public static String BINARY_RESULT_FILE_EXT = ".hopsbin";
    
    public static String FILES_TO_CRAETE_IN_WARM_UP_PHASE_KEY = "files.to.create.in.warmup.phase";
    public static int    FILES_TO_CRAETE_IN_WARM_UP_PHASE_DEFAULT = 10;
    
    public static String WARM_UP_PHASE_WAIT_TIME_KEY = "warmup.phase.wait.time";
    public static int    WARM_UP_PHASE_WAIT_TIME_DEFAULT = 1 * 60 * 1000;
    
    public static String LIST_OF_SLAVES_KEY = "list.of.slaves";
    public static String LIST_OF_SLAVES_DEFAULT = "localhost";
    
//    public static String NAME_NODE_LIST_KEY = "namenode.list";
//    public static String NAME_NODE_LIST_DEFAULT = "";
    
    public static String FS_DEFAULTFS_KEY = "fs.defaultFS";
    public static String FS_DEFAULTFS_DEFAULT = "";
    
    public static String DFS_NAMENODE_SELECTOR_POLICY_KEY="dfs.namenode.selector-policy";
    public static String DFS_NAMENODE_SELECTOR_POLICY_DEFAULT="RANDOM_STICKY";
    
    public static String DFS_CLIENT_REFRESH_NAMENODE_LIST_KEY="dsf.client.refresh.namenode.list";
    public static long   DFS_CLIENT_REFRESH_NAMENODE_LIST_DEFAULT=60*60*1000;
}
