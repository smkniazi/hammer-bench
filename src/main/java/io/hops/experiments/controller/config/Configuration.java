/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.experiments.controller.config;

import io.hops.experiments.benchmarks.blockreporting.TinyDatanodesHelper;
import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.*;

import io.hops.experiments.coin.MultiFaceCoin;
import io.hops.experiments.benchmarks.common.BenchmarkType;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 *
 * @author salman
 */
public class Configuration implements Serializable {

  private List<InetAddress> listOfSlaves = null;
  private List<String> nameNodeList = null;
  private Properties props = null;

  private Configuration() {
  }

  public static void printHelp() {
    System.out.println("FU");
  }

  public Configuration(String file) throws FileNotFoundException, IOException, SQLException {
    props = loadPropFile(file);
    validateArgs();
  }

  private Properties loadPropFile(String file) throws FileNotFoundException, IOException {
    final String PROP_FILE = file;
    Properties props = new Properties();
    InputStream input = new FileInputStream(PROP_FILE);
    props.load(input);
//    Enumeration<?> keys = props.propertyNames();
//    while(keys.hasMoreElements()){
//      String key = (String)keys.nextElement();
//      System.out.println("Key "+key+" Value: "+props.getProperty(key));
//    }
    return props;
  }

  private void validateArgs() throws UnknownHostException, SQLException {

    // check for the
    if (getRawBmFilesCreationPhaseDuration() <= 0 && getBenchMarkType() == BenchmarkType.RAW) {
      throw new IllegalArgumentException("You must write some files before testing other filesystem operations");
    }

    if (getInterleavedBmCreateFilesPercentage().doubleValue() <= 0 && getBenchMarkType() == BenchmarkType.INTERLEAVED) {
      throw new IllegalArgumentException("You must write some files before testing other filesystem operations");
    }

    if (getInterleavedBmCreateFilesPercentage().doubleValue() <= getInterleavedBmDeleteFilesPercentage().doubleValue() && getBenchMarkType() == BenchmarkType.INTERLEAVED) {
      throw new IllegalArgumentException("Delete operations can not be more than create operations");
    }

    if (getBenchMarkType() == BenchmarkType.INTERLEAVED) {
      //create a coin to check the percentages
      new MultiFaceCoin(getInterleavedBmCreateFilesPercentage(),
              getInterleavedBmAppendFilePercentage(),
              getInterleavedBmReadFilesPercentage(),
              getInterleavedBmRenameFilesPercentage(),
              getInterleavedBmDeleteFilesPercentage(), getInterleavedBmLsFilePercentage(),
              getInterleavedBmLsDirPercentage(), getInterleavedBmChmodFilesPercentage(),
              getInterleavedBmChmodDirsPercentage(), getInterleavedBmMkdirPercentage(),
              getInterleavedBmSetReplicationPercentage(),
              getInterleavedBmGetFileInfoPercentage(),
              getInterleavedBmGetDirInfoPercentage(),
              getInterleavedBmFileChangeOwnerPercentage(),
              getInterleavedBmDirChangeOwnerPercentage());
    }

    if (getBenchMarkType() == BenchmarkType.BR
            && (getBenchMarkFileSystemName() != BenchMarkFileSystemName.HDFS
            && getBenchMarkFileSystemName() != BenchMarkFileSystemName.HopsFS)) {
      throw new IllegalStateException("Block report benchmark is only supported for HDFS and HopsFS");
    }

    if(testFailover()){
      if(getBenchMarkType() != BenchmarkType.INTERLEAVED){
        throw new IllegalArgumentException("Failover Testing is only supported for interleaved benchmark");
      }
      if(getBenchMarkFileSystemName() != BenchMarkFileSystemName.HDFS && getBenchMarkFileSystemName() != BenchMarkFileSystemName.HopsFS){
        throw new IllegalArgumentException("Failover Testing is only supported for HDFS and HopsFS.");
      }
//      if(getSlavesList().size()!=1){
//        throw new IllegalArgumentException("Failover Testing is only supported with one slave.");
//      }
      if(getHadoopUser()==null){
        throw new IllegalArgumentException("Hadoop user is not set.");
      }
      if(getHadoopSbin()==null){
        throw new IllegalArgumentException("Hadoop sbin folder is not set.");
      }
      if(getFailOverNameNodes().size()==0){
        throw new IllegalArgumentException("Hadoop namenodes are not set.");
      }
      if(getNameNodeRestartCommands().size()==0){
        throw new IllegalArgumentException("Hadoop failover commands are not set properly.");
      }
      if(getFailOverTestStartTime() > getFailOverTestDuration()){
        throw new IllegalArgumentException("Failover start time can not be greater than failover test duration");
      }
      if(getInterleavedBmDuration() < (getFailOverTestStartTime()+getFailOverTestDuration())){
        throw new IllegalArgumentException(ConfigKeys.FAIL_OVER_TEST_DURATION_KEY+" + "+ConfigKeys.FAIL_OVER_TEST_START_TIME_KEY
                +" should be greater than "+ConfigKeys.INTERLEAVED_BM_DURATION_KEY);
      }
    }

    if(!isBlockReportingSkipCreations() && getBenchMarkType() == BenchmarkType.BR) {
      TinyDatanodesHelper.dropTable(getBlockReportingPersistDatabase());
    }
  }

  public List<InetAddress> getSlavesList() throws UnknownHostException {
    if (listOfSlaves == null) {
      listOfSlaves = new ArrayList<InetAddress>();
      String listOfSlavesString = getString(ConfigKeys.LIST_OF_SLAVES_KEY, ConfigKeys.LIST_OF_SLAVES_DEFAULT);
      StringTokenizer st = new StringTokenizer(listOfSlavesString, ", ");
      while (st.hasMoreTokens()) {
        String slaveAddress = st.nextToken();
        listOfSlaves.add(InetAddress.getByName(slaveAddress));
      }
    }
    return listOfSlaves;
  }

  public BenchMarkFileSystemName getBenchMarkFileSystemName() {
    return BenchMarkFileSystemName.fromString(getString(ConfigKeys.BENCHMARK_FILE_SYSTEM_NAME_KEY, ConfigKeys.BENCHMARK_FILE_SYSTEM_NAME_DEFAULT));
  }

  public int getSlaveListeningPort() {
    return getInt(ConfigKeys.SLAVE_LISTENING_PORT_KEY, ConfigKeys.SLAVE_LISTENING_PORT_DEFAULT);
  }

  public int getMasterListeningPort() {
    return getInt(ConfigKeys.MASTER_LISTENING_PORT_KEY, ConfigKeys.MASTER_LISTENING_PORT_DEFAULT);
  }

  public BenchmarkType getBenchMarkType() {
    String val = getString(ConfigKeys.BENCHMARK_TYPE_KEY, ConfigKeys.BENCHMARK_TYPE_DEFAULT);
    return BenchmarkType.valueOf(val);
  }

  public int getNamenodeCount() {
    return getInt(ConfigKeys.NO_OF_NAMENODES_KEY, ConfigKeys.NO_OF_NAMENODES_DEFAULT);
  }

  public int getNdbNodesCount() {
    return getInt(ConfigKeys.NO_OF_NDB_DATANODES_KEY, ConfigKeys.NO_OF_NDB_DATANODES_DEFAULT);
  }

  public long getRawBmFilesCreationPhaseDuration() {
    return getLong(ConfigKeys.RAW_CREATE_FILES_PHASE_DURATION_KEY, ConfigKeys.RAW_CREATE_FILES_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmCreateFilesPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_CREATE_FILES_PERCENTAGE_KEY, ConfigKeys.INTLVD_CREATE_FILES_PERCENTAGE_DEFAULT);
  }

  public long getRawBmMaxFilesToCreate() {
    return getLong(ConfigKeys.RAW_CREATE_PHASE_MAX_FILES_TO_CRAETE_KEY, ConfigKeys.RAW_CREATE_PHASE_MAX_FILES_TO_CRAETE_DEFAULT);
  }

  public long getRawBmReadFilesPhaseDuration() {
    return getLong(ConfigKeys.RAW_READ_FILES_PHASE_DURATION_KEY, ConfigKeys.RAW_READ_FILES_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmReadFilesPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_READ_FILES_PERCENTAGE_KEY, ConfigKeys.INTLVD_READ_FILES_PERCENTAGE_DEFAULT);
  }

  public long getRawBmRenameFilesPhaseDuration() {
    return getLong(ConfigKeys.RAW_RENAME_FILES_PHASE_DURATION_KEY, ConfigKeys.RAW_RENAME_FILES_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmRenameFilesPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_RENAME_FILES_PERCENTAGE_KEY, ConfigKeys.INTLVD_RENAME_FILES_PERCENTAGE_DEFAULT);
  }

  public long getRawBmDeleteFilesPhaseDuration() {
    return getLong(ConfigKeys.RAW_DElETE_FILES_PHASE_DURATION_KEY, ConfigKeys.RAW_DELETE_FILES_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmDeleteFilesPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_DELETE_FILES_PERCENTAGE_KEY, ConfigKeys.INTLVD_DELETE_FILES_PERCENTAGE_DEFAULT);
  }

  public long getRawBmChmodFilesPhaseDuration() {
    return getLong(ConfigKeys.RAW_CHMOD_FILES_PHASE_DURATION_KEY, ConfigKeys.RAW_CHMOD_FILES_PHASE_DURATION_DEFAULT);
  }

  public long getRawBmChmodDirsPhaseDuration() {
    return getLong(ConfigKeys.RAW_CHMOD_DIRS_PHASE_DURATION_KEY, ConfigKeys.RAW_CHMOD_DIRS_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmChmodFilesPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_CHMOD_FILES_PERCENTAGE_KEY, ConfigKeys.INTLVD_CHMOD_FILES_PERCENTAGE_DEFAULT);
  }

  public BigDecimal getInterleavedBmChmodDirsPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_CHMOD_DIRS_PERCENTAGE_KEY, ConfigKeys.INTLVD_CHMOD_DIRS_PERCENTAGE_DEFAULT);
  }

  public long getRawBmLsFilePhaseDuration() {
    return getLong(ConfigKeys.RAW_LS_FILE_PHASE_DURATION_KEY, ConfigKeys.RAW_LS_FILE_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmLsFilePercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_LS_FILE_PERCENTAGE_KEY, ConfigKeys.INTLVD_LS_FILE_PERCENTAGE_DEFAULT);
  }

  public long getRawBmLsDirPhaseDuration() {
    return getLong(ConfigKeys.RAW_LS_DIR_PHASE_DURATION_KEY, ConfigKeys.RAW_LS_DIR_PHASE_DURATION_DEFAULT);
  }
  public static String INTERLEAVED_WORKLOAD_NAME_KEY = "interleaved.workload.name";
  public static double INTERLEAVED_WORKLOAD_NAME_DEFAULT = 0;

  public String getInterleavedBmWorkloadName() {
    return getString(ConfigKeys.INTERLEAVED_WORKLOAD_NAME_KEY, ConfigKeys.INTERLEAVED_WORKLOAD_NAME_DEFAULT);
  }

  public BigDecimal getInterleavedBmLsDirPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_LS_DIR_PERCENTAGE_KEY, ConfigKeys.INTLVD_LS_DIR_PERCENTAGE_DEFAULT);
  }

  public long getRawBmMkdirPhaseDuration() {
    return getLong(ConfigKeys.RAW_MKDIR_PHASE_DURATION_KEY, ConfigKeys.RAW_MKDIR_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmMkdirPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_MKDIR_PERCENTAGE_KEY, ConfigKeys.INTLVD_MKDIR_PERCENTAGE_DEFAULT);
  }

  public long getRawBmSetReplicationPhaseDuration() {
    return getLong(ConfigKeys.RAW_SETREPLICATION_PHASE_DURATION_KEY, ConfigKeys.RAW_SETREPLICATION_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmSetReplicationPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_SETREPLICATION_PERCENTAGE_KEY, ConfigKeys.INTLVD_SETREPLICATION_PERCENTAGE_DEFAULT);
  }

  public long getRawBmGetFileInfoPhaseDuration() {
    return getLong(ConfigKeys.RAW_GET_FILE_INFO_PHASE_DURATION_KEY, ConfigKeys.RAW_GET_FILE_INFO_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmGetFileInfoPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_GET_FILE_INFO_PERCENTAGE_KEY, ConfigKeys.INTLVD_GET_FILE_INFO_PERCENTAGE_DEFAULT);
  }

  public long getRawBmGetDirInfoPhaseDuration() {
    return getLong(ConfigKeys.RAW_GET_DIR_INFO_PHASE_DURATION_KEY, ConfigKeys.RAW_GET_DIR_INFO_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmGetDirInfoPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_GET_DIR_INFO_PERCENTAGE_KEY, ConfigKeys.INTLVD_GET_DIR_INFO_PERCENTAGE_DEFAULT);
  }

  public long getRawBmAppendFilePhaseDuration() {
    return getLong(ConfigKeys.RAW_FILE_APPEND_PHASE_DURATION_KEY, ConfigKeys.RAW_FILE_APPEND_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmAppendFilePercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_APPEND_FILE_PERCENTAGE_KEY, ConfigKeys.INTLVD_APPEND_FILE_PERCENTAGE_DEFAULT);
  }

  public int getBlockReportBenchMarkDuration() {
    return getInt(ConfigKeys.BR_BENCHMARK_DURATION_KEY, ConfigKeys.BR_BENCHMARK_DURATION_DEFAULT);
  }

  public int getBlockReportingNumOfBlocksPerReport() {
    return getInt(ConfigKeys.BR_NUM_BLOCKS_PER_REPORT, ConfigKeys.BR_NUM_BLOCKS_PER_REPORT_DEFAULT);
  }

  public int getBlockReportingNumOfBlocksPerFile() {
    return getInt(ConfigKeys.BR_NUM_BLOCKS_PER_FILE, ConfigKeys.BR_NUM_BLOCKS_PER_FILE_DEFAULT);
  }

  public int getBlockReportingMaxBlockSize() {
    return getInt(ConfigKeys.BR_MAX_BLOCK_SIZE, ConfigKeys.BR_MAX_BLOCK_SIZE_DEFAULT);
  }

  public int getBlockReportingNumOfFilesPerDir() {
    return getInt(ConfigKeys.BR_NUM_FILES_PER_DIR, ConfigKeys.BR_NUM_FILES_PER_DIR_DEFAULT);
  }

  public boolean isBlockReportingSkipCreations() {
    return getBoolean(ConfigKeys.BR_SKIP_CREATIONS, ConfigKeys.BR_SKIP_CREATIONS_DEFAULT);
  }

  public int getBlockReportingMaxTimeBeforeNextReport() {
    return getInt(ConfigKeys.BR_MAX_TIME_BEFORE_NEXT_REPORT, ConfigKeys.BR_MAX_TIME_BEFORE_NEXT_REPORT_DEFAULT);
  }

  public int getBlockReportingMinTimeBeforeNextReport() {
    return getInt(ConfigKeys.BR_MIN_TIME_BEFORE_NEXT_REPORT, ConfigKeys.BR_MIN_TIME_BEFORE_NEXT_REPORT_DEFAULT);
  }

  public String getBlockReportingPersistDatabase() {
    return getString(ConfigKeys.BR_PERSIST_DATABASE, ConfigKeys.BR_PERSIST_DATABASE_DEFAULT);
  }

  public short getReplicationFactor() {
    return getShort(ConfigKeys.REPLICATION_FACTOR_KEY, ConfigKeys.REPLICATION_FACTOR_DEFAULT);
  }

  public long getFileSize() {
    return getLong(ConfigKeys.FILE_SIZE_IN_Bytes_KEY, ConfigKeys.FILE_SIZE_IN_Bytes_DEFAULT);
  }

  public long getAppendFileSize() {
    return getLong(ConfigKeys.APPEND_FILE_SIZE_IN_Bytes_KEY, ConfigKeys.APPEND_FILE_SIZE_IN_Bytes_DEFAULT);
  }

  public int getSlaveNumThreads() {
    return getInt(ConfigKeys.NUM_SLAVE_THREADS_KEY, ConfigKeys.NUM_SLAVE_THREADS_DEFAULT);
  }

  public String getBaseDir() {
    return getString(ConfigKeys.BASE_DIR_KEY, ConfigKeys.BASE_DIR_DEFAULT);
  }

  public boolean isSkipAllPrompt() {
    return getBoolean(ConfigKeys.SKIP_ALL_PROMPT_KEY, ConfigKeys.SKIP_ALL_PROMPT_DEFAULT);
  }

  public boolean isEnableRemoteLogging() {
    return getBoolean(ConfigKeys.ENABLE_REMOTE_LOGGING_KEY, ConfigKeys.ENABLE_REMOTE_LOGGING_DEFAULT);
  }

  public int getRemoteLogginPort() {
    return getInt(ConfigKeys.REMOTE_LOGGING_PORT_KEY, ConfigKeys.REMOTE_LOGGING_PORT_DEFAULT);
  }

  public String getResultsDir() {
    return getString(ConfigKeys.RESULTS_DIR_KEY, ConfigKeys.RESULTS_DIR_DEFAULT);
  }

  public int getFilesToCreateInWarmUpPhase() {
    return getInt(ConfigKeys.FILES_TO_CRAETE_IN_WARM_UP_PHASE_KEY, ConfigKeys.FILES_TO_CRAETE_IN_WARM_UP_PHASE_DEFAULT);
  }

  public long getInterleavedBmDuration() {
    return getLong(ConfigKeys.INTERLEAVED_BM_DURATION_KEY, ConfigKeys.INTERLEAVED_BM_DURATION_DEFAULT);
  }

  public int getWarmUpPhaseWaitTime() {
    return getInt(ConfigKeys.WARM_UP_PHASE_WAIT_TIME_KEY, ConfigKeys.WARM_UP_PHASE_WAIT_TIME_DEFAULT);
  }

  public String getNameNodeRpcAddress() {
    return getString(ConfigKeys.FS_DEFAULTFS_KEY, ConfigKeys.FS_DEFAULTFS_DEFAULT);
  }

  public String getNameNodeSelectorPolicy() {
    return getString(ConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY, ConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_DEFAULT);
  }

  public long getNameNodeRefreshRate() {
    return getLong(ConfigKeys.DFS_CLIENT_REFRESH_NAMENODE_LIST_KEY, ConfigKeys.DFS_CLIENT_REFRESH_NAMENODE_LIST_DEFAULT);
  }

  public int getDirPerDir() {
    return getInt(ConfigKeys.DIR_PER_DIR_KEY, ConfigKeys.DIR_PER_DIR_DEFAULT);
  }

  public int getFilesPerDir() {
    return getInt(ConfigKeys.FILES_PER_DIR_KEY, ConfigKeys.FILES_PER_DIR_DEFAULT);
  }

  public boolean isFixedDepthTree() {
    return getBoolean(ConfigKeys.ENABLE_FIXED_DEPTH_TREE_KEY, ConfigKeys.ENABLE_FIXED_DEPTH_TREE_DEFAULT);
  }

  public int getTreeDepth() {
    return getInt(ConfigKeys.TREE_DEPTH_KEY, ConfigKeys.TREE_DEPTH_DEFAULT);
  }

  public long getRawFileChangeUserPhaseDuration() {
    return getLong(ConfigKeys.RAW_FILE_CHANGE_USER_PHASE_DURATION_KEY, ConfigKeys.RAW_FILE_CHANGE_USER_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmFileChangeOwnerPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_FILE_CHANGE_USER_PERCENTAGE_KEY, ConfigKeys.INTLVD_FILE_CHANGE_USER_PERCENTAGE_DEFAULT);
  }

  public long getRawDirChangeUserPhaseDuration() {
    return getLong(ConfigKeys.RAW_DIR_CHANGE_USER_PHASE_DURATION_KEY, ConfigKeys.RAW_DIR_CHANGE_USER_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedBmDirChangeOwnerPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_DIR_CHANGE_USER_PERCENTAGE_KEY, ConfigKeys.INTLVD_DIR_CHANGE_USER_PERCENTAGE_DEFAULT);
  }

  public int getMaxSlavesFailureThreshold() {
    return getInt(ConfigKeys.MAX_SLAVE_FAILURE_THREASHOLD_KEY, ConfigKeys.MAX_SLAVE_FAILURE_THREASHOLD_DEFAULT);
  }

  public boolean isPercentileEnabled() {
    return getBoolean(ConfigKeys.GENERATE_PERCENTILES_KEY, ConfigKeys.GENERATE_PERCENTILES_DEFAULT);
  }

  public String getFsCephImp() {
    return getString(ConfigKeys.FS_CEPH_IMPL_KEY, ConfigKeys.FS_CEPH_IMPL_DEFAULT);
  }

  public String getCephAuthKeyRing() {
    return getString(ConfigKeys.CEPH_AUTH_KEYRING_KEY, ConfigKeys.CEPH_AUTH_KEYRING_DEFAULT);
  }

  public String getCephConfigFile() {
    return getString(ConfigKeys.CEPH_CONF_FILE_KEY, ConfigKeys.CEPH_CONF_FILE_DEFAULT);
  }

  public String getCephRootDir() {
    return getString(ConfigKeys.CEPH_ROOT_DIR_KEY, ConfigKeys.CEPH_ROOT_DIR_DEFAULT);
  }

  public String getCephMonAddress() {
    return getString(ConfigKeys.CEPH_MON_ADDRESS_KEY, ConfigKeys.CEPH_MON_ADDRESS_DEFAULT);
  }

  public String getCephAuthId() {
    return getString(ConfigKeys.CEPH_AUTH_ID_KEY, ConfigKeys.CEPH_AUTH_ID_DEFAULT);
  }


  public boolean testFailover(){
    return getBoolean(ConfigKeys.TEST_FAILOVER, ConfigKeys.TEST_FAILOVER_DEFAULT);
  }

  public long getNameNodeRestartTimePeriod(){
    return getLong(ConfigKeys.RESTART_NAMENODE_AFTER_KEY, ConfigKeys.RESTART_NAMENODE_AFTER_DEFAULT);
  }

  public long getFailOverTestStartTime(){
    return getLong(ConfigKeys.FAIL_OVER_TEST_START_TIME_KEY, ConfigKeys.FAIL_OVER_TEST_START_TIME_DEFAULT);
  }

  public long getFailOverTestDuration(){
    return getLong(ConfigKeys.FAIL_OVER_TEST_DURATION_KEY, ConfigKeys.FAIL_OVER_TEST_DURATION_DEFAULT);
  }

  public String getNamenodeKillerHost(){
    return getString(ConfigKeys.NAMENOE_KILLER_HOST_KEY, ConfigKeys.NAMENOE_KILLER_HOST_DEFAULT);
  }

  public List<String> getFailOverNameNodes(){
    List<String> namenodesList = new LinkedList<String>();
    String namenodes = getString(ConfigKeys.FAILOVER_NAMENODES,ConfigKeys.FAILOVER_NAMENODES_DEFAULT);
    if(namenodes != null){
      StringTokenizer st = new StringTokenizer(namenodes, ",");
      while(st.hasMoreElements()){
        String namenode = st.nextToken();
        namenodesList.add(namenode);
      }
    }
    return namenodesList;
  }

  public String getHadoopSbin(){
    return getString(ConfigKeys.HADOOP_SBIN, ConfigKeys.HADOOP_SBIN_DEFAULT);
  }

  public String getHadoopUser(){
    return getString(ConfigKeys.HADOOP_USER,ConfigKeys.HADOOP_USER_DEFAULT);
  }

  public int getSlaveWarmUpDelay(){
    return getInt(ConfigKeys.MASTER_SLAVE_WARMUP_DELAY_KEY, ConfigKeys.MASTER_SLAVE_WARMUP_DELAY_KEY_DEFAULT);
  }

  public List<List<String>> getNameNodeRestartCommands(){
    List<List<String>> commandsPerNN = new ArrayList<List<String>>();

    String commandsStr = getString(ConfigKeys.NAMENOE_RESTART_COMMANDS,ConfigKeys.NAMENOE_RESTART_COMMANDS_DEFAULT);

    if(commandsStr!=null) {
      if (getHadoopSbin() != null) {
        commandsStr = commandsStr.replaceAll("HADOOP_SBIN", getHadoopSbin());
      }

      if (getHadoopUser() != null) {
        commandsStr = commandsStr.replaceAll("HADOOP_USER", getHadoopUser());
      }

      for(String namenode: getFailOverNameNodes()){
        String commandTmp = new String(commandsStr);
        commandTmp = commandTmp.replaceAll("NAMENODE", namenode);

        StringTokenizer st = new StringTokenizer(commandTmp, ",");
        List<String> commands = new LinkedList<String>();
        while(st.hasMoreElements()){
          commands.add(st.nextToken());
        }
        commandsPerNN.add(commands);
        }
      }

    return commandsPerNN;
  }

  public String getDfsNameService(){
    return getString(ConfigKeys.DFS_NAMESERVICES, ConfigKeys.DFS_NAMESERVICES_DEFAULT);
  }

  public Properties getFsConfig() {
    Properties dfsClientConf = new Properties();
    dfsClientConf.setProperty(ConfigKeys.FS_DEFAULTFS_KEY, getNameNodeRpcAddress());
    if (getBenchMarkFileSystemName() == BenchMarkFileSystemName.HDFS) {
      System.out.println("Creating config for HDFS");
      dfsClientConf.setProperty("dfs.ha.namenodes."+getDfsNameService(),props.getProperty("dfs.ha.namenodes."+getDfsNameService()));
      dfsClientConf.setProperty("dfs.nameservices",props.getProperty("dfs.nameservices"));
      dfsClientConf.setProperty("dfs.namenode.rpc-address."+getDfsNameService()+".nn1",props.getProperty("dfs.namenode.rpc-address."+getDfsNameService()+".nn1"));
      dfsClientConf.setProperty("dfs.namenode.rpc-address."+getDfsNameService()+".nn2",props.getProperty("dfs.namenode.rpc-address."+getDfsNameService()+".nn2"));
      dfsClientConf.setProperty("dfs.client.failover.proxy.provider."+getDfsNameService(),props.getProperty("dfs.client.failover.proxy.provider."+getDfsNameService()));
    } else if (getBenchMarkFileSystemName() == BenchMarkFileSystemName.HopsFS) {
      System.out.println("Creating config for HopsFS");
      dfsClientConf.setProperty(ConfigKeys.DFS_CLIENT_REFRESH_NAMENODE_LIST_KEY,
              Long.toString(getNameNodeRefreshRate()));
      dfsClientConf.setProperty(ConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY,
              getNameNodeSelectorPolicy());
      dfsClientConf.setProperty(ConfigKeys.DFS_CLIENT_MAX_RETRIES_ON_FAILURE_KEY,
              Integer.toString(getInt(ConfigKeys.DFS_CLIENT_MAX_RETRIES_ON_FAILURE_KEY,ConfigKeys.DFS_CLIENT_MAX_RETRIES_ON_FAILURE_DEFAULT)));
      dfsClientConf.setProperty(ConfigKeys.DFS_CLIENT_INITIAL_WAIT_ON_FAILURE_KEY,
              Long.toString(getLong(ConfigKeys.DFS_CLIENT_INITIAL_WAIT_ON_FAILURE_KEY,ConfigKeys.DFS_CLIENT_INITIAL_WAIT_ON_FAILURE_DEFAULT)));
      dfsClientConf.setProperty(ConfigKeys.DFS_STORE_SMALL_FILES_IN_DB,
              Boolean.toString(getBoolean(ConfigKeys.DFS_STORE_SMALL_FILES_IN_DB, ConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_DEFAULT)));
      dfsClientConf.setProperty(ConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY,
              Integer.toString(getInt(ConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, ConfigKeys.DFS_DB_FILE_MAX_SIZE_DEFAULT)));
    } else if (getBenchMarkFileSystemName() == BenchMarkFileSystemName.CephFS) {
      System.out.println("Creating config for CephFS");
      dfsClientConf.setProperty(ConfigKeys.FS_CEPH_IMPL_KEY, getFsCephImp());
      dfsClientConf.setProperty(ConfigKeys.CEPH_AUTH_KEYRING_KEY, getCephAuthKeyRing());
      dfsClientConf.setProperty(ConfigKeys.CEPH_CONF_FILE_KEY, getCephConfigFile());
      dfsClientConf.setProperty(ConfigKeys.CEPH_ROOT_DIR_KEY, getCephRootDir());
      dfsClientConf.setProperty(ConfigKeys.CEPH_MON_ADDRESS_KEY, getCephMonAddress());
      dfsClientConf.setProperty(ConfigKeys.CEPH_AUTH_ID_KEY, getCephAuthId());
    } else if (getBenchMarkFileSystemName() == BenchMarkFileSystemName.MapRFS) {
      System.out.println("Creating config for MapR-FS");
      //FS_DEFAULTFS_KEY is already defined
    } else {
      throw new UnsupportedOperationException(getBenchMarkFileSystemName() + " is not yet supported");
    }
    return dfsClientConf;
  }

  private int getInt(String key, int defaultVal) {
    String val = props.getProperty(key, Integer.toString(defaultVal));
    return Integer.parseInt(val);
  }

  private long getLong(String key, long defaultVal) {
    String val = props.getProperty(key, Long.toString(defaultVal));
    return Long.parseLong(val);
  }

  private short getShort(String key, short defaultVal) {
    String val = props.getProperty(key, Short.toString(defaultVal));
    return Short.parseShort(val);
  }

  private boolean getBoolean(String key, boolean defaultVal) {
    String val = props.getProperty(key, Boolean.toString(defaultVal));
    return Boolean.parseBoolean(val);
  }

  private String getString(String key, String defaultVal) {
    String val = props.getProperty(key, defaultVal);
    if(val != null){
      val.trim();
    }
    return val;
  }

  private BigDecimal getBigDecimal(String key, double defaultVal) {
    if (!isTwoDecimalPlace(defaultVal)) {
      throw new IllegalArgumentException("Wrong default Value. Only one decimal place is supported. Value " + defaultVal + " key: " + key);
    }

    double userVal = Double.parseDouble(props.getProperty(key, Double.toString(defaultVal)));
    if (!isTwoDecimalPlace(userVal)) {
      throw new IllegalArgumentException("Wrong user value. Only one decimal place is supported. Value " + userVal + " key: " + key);
    }

    //System.out.println(key+" value "+userVal);
    return new BigDecimal(userVal, new MathContext(4, RoundingMode.HALF_UP));
  }

  private boolean isTwoDecimalPlace(double val) {
    if (val == 0 || val == ((int) val)) {
      return true;
    } else {
      String valStr = Double.toString(val);
      int i = valStr.lastIndexOf('.');
      if (i != -1 && (valStr.substring(i + 1).length() == 1 || valStr.substring(i + 1).length() == 2)) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   *
   * @author salman
   */
  public static class ConfigKeys {

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

      public static String FILE_SIZE_IN_Bytes_KEY= "file.size";
      public static long   FILE_SIZE_IN_Bytes_DEFAULT = 0;

      public static String APPEND_FILE_SIZE_IN_Bytes_KEY= "append.size";
      public static long   APPEND_FILE_SIZE_IN_Bytes_DEFAULT = 0;

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

      public static final String DFS_DB_FILE_MAX_SIZE_KEY = "dfs.database.file.max.size";
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
}
