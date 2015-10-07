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
package io.hops.experiments.controller;

import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import io.hops.experiments.coin.MultiFaceCoin;
import io.hops.experiments.benchmarks.common.BenchmarkType;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 *
 * @author salman
 */
public class MasterArgsReader {

  private List<InetAddress> listOfSlaves = null;
  private List<String> nameNodeList = null;
  private Properties props = null;

  private MasterArgsReader() {
  }

  public static void printHelp() {
    System.out.println("FU");
  }

  public MasterArgsReader(String file) throws FileNotFoundException, IOException {
    props = loadPropFile(file);
    validateArgs();
  }

  private Properties loadPropFile(String file) throws FileNotFoundException, IOException {
    final String PROP_FILE = file;
    Properties props = new Properties();
    InputStream input = new FileInputStream(PROP_FILE);
    props.load(input);
    return props;
  }

  private void validateArgs() throws UnknownHostException {

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


    switch (getBenchMarkType()) {
      case INTERLEAVED:
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
                getInterleavedFileChangeUserPercentage(),
                getInterleavedDirChangeUserPercentage());
        break;
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

  public int getBLockReportingNumOfReports() {
    return getInt(ConfigKeys.BR_NUM_REPORTS, ConfigKeys.BR_NUM_REPORTS_DEFAULT);
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

  public BigDecimal getInterleavedFileChangeUserPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_FILE_CHANGE_USER_PERCENTAGE_KEY, ConfigKeys.INTLVD_FILE_CHANGE_USER_PERCENTAGE_DEFAULT);
  }

  public long getRawDirChangeUserPhaseDuration() {
    return getLong(ConfigKeys.RAW_DIR_CHANGE_USER_PHASE_DURATION_KEY, ConfigKeys.RAW_DIR_CHANGE_USER_PHASE_DURATION_DEFAULT);
  }

  public BigDecimal getInterleavedDirChangeUserPercentage() {
    return getBigDecimal(ConfigKeys.INTLVD_DIR_CHANGE_USER_PERCENTAGE_KEY, ConfigKeys.INTLVD_DIR_CHANGE_USER_PERCENTAGE_DEFAULT);
  }

  public int getMaxSlavesFailureThreshold() {
    return getInt(ConfigKeys.MAX_SLAVE_FAILURE_THREASHOLD_KEY, ConfigKeys.MAX_SLAVE_FAILURE_THREASHOLD_DEFAULT);
  }

  public boolean isPercentileEnabled() {
    return getBoolean(ConfigKeys.GENERATE_PERCENTILES_KEY, ConfigKeys.GENERATE_PERCENTILES_DEFAULT);
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
    return props.getProperty(key, defaultVal);
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
}
