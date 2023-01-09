/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.experiments.benchmarks.common.config;

//import io.hops.experiments.benchmarks.blockreporting.TinyDatanodesHelper;

import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
import io.hops.experiments.benchmarks.common.BenchmarkType;
import io.hops.experiments.benchmarks.common.coin.FileSizeMultiFaceCoin;
import io.hops.experiments.benchmarks.interleaved.coin.InterleavedMultiFaceCoin;
import io.hops.experiments.utils.DFSOperationsUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * @author salman
 */
public class BMConfiguration implements Serializable {

  private List<InetAddress> listOfSlaves = null;
  private List<String> nameNodeList = null;
  private Properties props = null;

  private BMConfiguration() {
  }

  public static void printHelp() {
    System.out.println("You are doomed");
  }

  public BMConfiguration(String file) throws FileNotFoundException, IOException, SQLException {
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

    //check the file size distribution coin
    FileSizeMultiFaceCoin fcoin = new FileSizeMultiFaceCoin(this.getFileSizeDistribution());

    if (getBenchMarkType() == BenchmarkType.INTERLEAVED) {
      //create a coin to check the percentages
      new InterleavedMultiFaceCoin(getInterleavedBmCreateFilesPercentage(),
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

    if (testFailover()) {
      if (getBenchMarkType() != BenchmarkType.INTERLEAVED) {
        throw new IllegalArgumentException("Failover Testing is only supported for interleaved benchmark");
      }
      if (getBenchMarkFileSystemName() != BenchMarkFileSystemName.HopsFS) {
        throw new IllegalArgumentException("Failover Testing is only supported for HDFS and HopsFS.");
      }
//      if(getSlavesList().size()!=1){
//        throw new IllegalArgumentException("Failover Testing is only supported with one slave.");
//      }

      if (getHadoopUser() == null) {
        throw new IllegalArgumentException("Hadoop user is not set.");
      }
      if (getHadoopSbin() == null) {
        throw new IllegalArgumentException("Hadoop sbin folder is not set.");
      }
      if (getFailOverNameNodes().size() == 0) {
        throw new IllegalArgumentException("Hadoop namenodes are not set.");
      }
      if (getNameNodeRestartCommands().size() == 0) {
        throw new IllegalArgumentException("Hadoop failover commands are not set properly.");
      }
      if (getFailOverTestStartTime() > getFailOverTestDuration()) {
        throw new IllegalArgumentException("Failover start time can not be greater than failover test duration");
      }
      if (getInterleavedBmDuration() < (getFailOverTestStartTime() + getFailOverTestDuration())) {
        throw new IllegalArgumentException(ConfigKeys.FAIL_OVER_TEST_DURATION_KEY + " + " + ConfigKeys.FAIL_OVER_TEST_START_TIME_KEY
                + " should be greater than " + ConfigKeys.INTERLEAVED_BM_DURATION_KEY);
      }
    }

    if (getBRNumInvalidBuckets() > getNumBuckets()) {
      throw new IllegalArgumentException("Number of invalid buckets cannot be more than the " +
              "total number of the buckets");
    }

//    if(!brReadStateFromDisk() && getBenchMarkType() == BenchmarkType.BR) {
//      TinyDatanodesHelper.dropTable(getBlockReportingPersistDatabase());
//    }
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

  public int getBRNumInvalidBuckets() {
    return getInt(ConfigKeys.BR_NUM_INVALID_BUCKETS_KEY, ConfigKeys.BR_NUM_INVALID_BUCKETS_DEFAULT);
  }

  public short getReplicationFactor() {
    return getShort(ConfigKeys.REPLICATION_FACTOR_KEY, ConfigKeys.REPLICATION_FACTOR_DEFAULT);
  }

  public String getFileSizeDistribution() {
    return getString(ConfigKeys.FILE_SIZE_IN_Bytes_KEY, ConfigKeys.FILE_SIZE_IN_Bytes_DEFAULT);
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

  public boolean isSSLEnabled() {
    return getBoolean(ConfigKeys.IPC_SERVER_SSL_ENABLED_KEY, ConfigKeys.IPC_SERVER_SSL_ENABLED_DEFAULT);
  }

  public int getRemoteLoggingPort() {
    return getInt(ConfigKeys.REMOTE_LOGGING_PORT_KEY, ConfigKeys.REMOTE_LOGGING_PORT_DEFAULT);
  }

  public String getResultsDir() {
    String filePath = getString(ConfigKeys.RESULTS_DIR_KEY, ConfigKeys.RESULTS_DIR_DEFAULT);
    if (!filePath.endsWith("/")) {
      filePath += "/";
    }
    return filePath;
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

  public boolean testFailover() {
    return getBoolean(ConfigKeys.TEST_FAILOVER, ConfigKeys.TEST_FAILOVER_DEFAULT);
  }

  public long getNameNodeRestartTimePeriod() {
    return getLong(ConfigKeys.RESTART_NAMENODE_AFTER_KEY, ConfigKeys.RESTART_NAMENODE_AFTER_DEFAULT);
  }

  public long getFailOverTestStartTime() {
    return getLong(ConfigKeys.FAIL_OVER_TEST_START_TIME_KEY, ConfigKeys.FAIL_OVER_TEST_START_TIME_DEFAULT);
  }

  public long getFailOverTestDuration() {
    return getLong(ConfigKeys.FAIL_OVER_TEST_DURATION_KEY, ConfigKeys.FAIL_OVER_TEST_DURATION_DEFAULT);
  }

  public String getNamenodeKillerHost() {
    return getString(ConfigKeys.NAMENOE_KILLER_HOST_KEY, ConfigKeys.NAMENOE_KILLER_HOST_DEFAULT);
  }

  public boolean getReadFilesFromDisk() {
    return getBoolean(ConfigKeys.READ_FILES_FROM_DISK, ConfigKeys.READ_FILES_FROM_DISK_DEFAULT);
  }

  public String getDiskNameSpacePath() {
    return getString(ConfigKeys.DISK_FILES_PATH, ConfigKeys.DISK_FILES_PATH_DEFAULT);
  }

  public List<String> getFailOverNameNodes() {
    List<String> namenodesList = new LinkedList<String>();
    String namenodes = getString(ConfigKeys.FAILOVER_NAMENODES, ConfigKeys.FAILOVER_NAMENODES_DEFAULT);
    if (namenodes != null) {
      StringTokenizer st = new StringTokenizer(namenodes, ",");
      while (st.hasMoreElements()) {
        String namenode = st.nextToken();
        namenodesList.add(namenode);
      }
    }
    return namenodesList;
  }

  public String getHadoopSbin() {
    return getString(ConfigKeys.HADOOP_SBIN, ConfigKeys.HADOOP_SBIN_DEFAULT);
  }

  public String getHadoopUser() {
    return getString(ConfigKeys.HADOOP_USER, ConfigKeys.HADOOP_USER_DEFAULT);
  }

  public int getSlaveWarmUpDelay() {
    return getInt(ConfigKeys.MASTER_SLAVE_WARMUP_DELAY_KEY, ConfigKeys.MASTER_SLAVE_WARMUP_DELAY_KEY_DEFAULT);
  }

  public int getNumBuckets() {
    return getInt(ConfigKeys.BR_NUM_BUCKETS_KEY, ConfigKeys.BR_NUM_BUCKETS_DEFAULT);
  }

  public List<List<String>> getNameNodeRestartCommands() {
    List<List<String>> commandsPerNN = new ArrayList<List<String>>();

    String commandsStr = getString(ConfigKeys.NAMENOE_RESTART_COMMANDS, ConfigKeys.NAMENOE_RESTART_COMMANDS_DEFAULT);

    if (commandsStr != null) {
      if (getHadoopSbin() != null) {
        commandsStr = commandsStr.replaceAll("HADOOP_SBIN", getHadoopSbin());
      }

      if (getHadoopUser() != null) {
        commandsStr = commandsStr.replaceAll("HADOOP_USER", getHadoopUser());
      }

      for (String namenode : getFailOverNameNodes()) {
        String commandTmp = new String(commandsStr);
        commandTmp = commandTmp.replaceAll("NAMENODE", namenode);

        StringTokenizer st = new StringTokenizer(commandTmp, ",");
        List<String> commands = new LinkedList<String>();
        while (st.hasMoreElements()) {
          commands.add(st.nextToken());
        }
        commandsPerNN.add(commands);
      }
    }

    return commandsPerNN;
  }

  public Properties getFsConfig() {
    Properties dfsClientConf = new Properties();
    dfsClientConf.setProperty(ConfigKeys.FS_DEFAULTFS_KEY, getNameNodeRpcAddress());
    if (getBenchMarkFileSystemName() == BenchMarkFileSystemName.HopsFS) {
      System.out.println("Creating config for HopsFS");
      dfsClientConf.setProperty(ConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY,
              Integer.toString(getInt(ConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, ConfigKeys.DFS_DB_FILE_MAX_SIZE_DEFAULT)));

      if (isSSLEnabled()) {
        dfsClientConf.setProperty("ipc.server.ssl.enabled", "true");
        dfsClientConf.setProperty("hadoop.ssl.hostname.verifier", "ALLOW_ALL");
        dfsClientConf.setProperty("hadoop.rpc.socket.factory.class.default", "org.apache.hadoop.net.HopsSSLSocketFactory");
        dfsClientConf.setProperty("hadoop.ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1,SSLv3");
        dfsClientConf.setProperty("client.rpc.ssl.enabled.protocol", "TLSv1.2");
        dfsClientConf.setProperty("hops.tls.superuser-material-directory", "/srv/hops/super_crypto/${USER}");
        dfsClientConf.setProperty("client.materialize.directory", "/srv/hops/certs-dir/transient");
        dfsClientConf.setProperty("hadoop.proxyuser.hdfs.hosts", "*");
        dfsClientConf.setProperty("hadoop.proxyuser.hdfs.groups", "*");
      }
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
    if (val != null) {
      val.trim();
    }
    return val;
  }

  private BigDecimal getBigDecimal(String key, double defaultVal) {
    if (!DFSOperationsUtils.isTwoDecimalPlace(defaultVal)) {
      throw new IllegalArgumentException("Wrong default Value. Only one decimal place is supported. Value " + defaultVal + " key: " + key);
    }

    double userVal = Double.parseDouble(props.getProperty(key, Double.toString(defaultVal)));
    if (!DFSOperationsUtils.isTwoDecimalPlace(userVal)) {
      throw new IllegalArgumentException("Wrong user value. Only one decimal place is supported. Value " + userVal + " key: " + key);
    }

    //System.out.println(key+" value "+userVal);
    return new BigDecimal(userVal, new MathContext(4, RoundingMode.HALF_UP));
  }


}
