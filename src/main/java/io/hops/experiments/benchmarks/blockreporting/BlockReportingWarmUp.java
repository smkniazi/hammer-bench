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
package io.hops.experiments.benchmarks.blockreporting;

import io.hops.experiments.benchmarks.common.BenchmarkType;
import io.hops.experiments.controller.commands.WarmUpCommand;

public class BlockReportingWarmUp {
  public static class Request implements WarmUpCommand.Request{
    private final String baseDir;
    private final int blocksPerReport;
    private final int blocksPerFile;
    private final int filesPerDir;
    private final short replication;
    private final int maxBlockSize;
    private final boolean brReadStateFromDisk;
    private final String databaseConnection;
    private final boolean ignoreBRLoadBalancer;
    private final int numBuckets;

    public Request(String baseDir, int blocksPerReport,
        int blocksPerFile, int filesPerDir, short replication, int
        maxBlockSize, boolean brReadStateFromDisk, String databaseConnection,
        boolean ignoreBRLoadBalancer, int numBuckets) {
      this.baseDir = baseDir;
      this.blocksPerReport = blocksPerReport;
      this.blocksPerFile = blocksPerFile;
      this.filesPerDir = filesPerDir;
      this.replication = replication;
      this.maxBlockSize = maxBlockSize;
      this.brReadStateFromDisk = brReadStateFromDisk;
      this.databaseConnection = databaseConnection;
      this.ignoreBRLoadBalancer = ignoreBRLoadBalancer;
      this.numBuckets = numBuckets;
    }

    public String getDatabaseConnection() {
      return databaseConnection;
    }

    public String getBaseDir() {
      return baseDir;
    }

    public int getBlocksPerReport() {
      return blocksPerReport;
    }

    public int getBlocksPerFile() {
      return blocksPerFile;
    }

    public int getReplication() {
      return replication;
    }

    public int getMaxBlockSize() {
      return maxBlockSize;
    }

    public int getFilesPerDir() {
      return filesPerDir;
    }

    public boolean brReadStateFromDisk() {
      return brReadStateFromDisk;
    }

    public boolean ignoreBRLoadBalancer() {
      return ignoreBRLoadBalancer;
    }

    public int getNumBuckets() {
      return numBuckets;
    }

    @Override
    public BenchmarkType getBenchMarkType() {
      return BenchmarkType.BR;
    }
  }

  public static class Response implements WarmUpCommand.Response{

  }
}
