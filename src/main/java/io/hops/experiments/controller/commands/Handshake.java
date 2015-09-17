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
package io.hops.experiments.controller.commands;

import io.hops.experiments.benchmarks.common.BenchmarkType;

import java.io.Serializable;

/**
 *
 * @author salman
 */
public class Handshake implements Serializable {

  public static class Request implements Serializable {

    private int slaveId;
    private final int numThreads;
    private final long fileSize;
    private final long appendSize;
    private final short replicationFactor;
    private BenchmarkType benchMarkType;
    private String baseDir;
    private boolean enableRemoteLogging;
    private int remoteLoggingPort;
    private String namenodeRpcAddress;
    private String namenodeSelectionPolicy;
    private long nameNodeListRefreshTime;
    private final int dirPerDir;
    private final int filesPerDir;
    private final long maxFilesToCreate;
    private final boolean fixedDepthTree;
    private final int treeDepth;

    public Request(int numThreads, long fileSize, long appendSize, short replicationFactor,
            BenchmarkType benchMarkType, String baseDir,
            boolean enableRemoteLogging, int remoteLoggingPort,
            String namenodeRpcAddress, String namenodeSelectionPolicy,
            long nameNodeListRefreshTime,
            int dirPerDir, int filesPerDir,
            long maxFilesToCreate,
            boolean fixedDepthTree,
            int treeDepth) {
      this.numThreads = numThreads;
      this.fileSize = fileSize;
      this.appendSize = appendSize;
      this.benchMarkType = benchMarkType;
      this.replicationFactor = replicationFactor;
      this.baseDir = baseDir;
      this.enableRemoteLogging = enableRemoteLogging;
      this.remoteLoggingPort = remoteLoggingPort;
      this.nameNodeListRefreshTime = nameNodeListRefreshTime;
      this.namenodeSelectionPolicy = namenodeSelectionPolicy;
      this.namenodeRpcAddress = namenodeRpcAddress;
      this.dirPerDir = dirPerDir;
      this.filesPerDir = filesPerDir;
      this.maxFilesToCreate = maxFilesToCreate;
      this.fixedDepthTree = fixedDepthTree;
      this.treeDepth = treeDepth;
    }

    public boolean isFixedDepthTree() {
      return fixedDepthTree;
    }

    public int getTreeDepth() {
      return treeDepth;
    }
    

    public long getMaxFilesToCreate() {
      return maxFilesToCreate;
    }

    public int getFilesPerDir() {
      return filesPerDir;
    }

    public int getNumThreads() {
      return numThreads;
    }

    public long getFileSize() {
      return fileSize;
    }

    public BenchmarkType getBenchMarkType() {
      return benchMarkType;
    }

    public short getReplicationFactor() {
      return replicationFactor;
    }

    public String getBaseDir() {
      return baseDir;
    }

    public boolean isEnableRemoteLogging() {
      return enableRemoteLogging;
    }

    public int getRemoteLoggingPort() {
      return remoteLoggingPort;
    }

    public String getNamenodeRpcAddress() {
      return namenodeRpcAddress;
    }

    public String getNamenodeSelectionPolicy() {
      return namenodeSelectionPolicy;
    }

    public long getNameNodeListRefreshTime() {
      return nameNodeListRefreshTime;
    }

    public int getSlaveId() {
      return slaveId;
    }

    public void setSlaveId(int slaveId) {
      this.slaveId = slaveId;
    }

    public long getAppendSize() {
      return appendSize;
    }

    public int getDirPerDir() {
      return dirPerDir;
    }
  }
  public static class Response implements Serializable {
  }
}
