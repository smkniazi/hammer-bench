/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.experiments.benchmarks.interleaved;

import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.common.config.BMConfiguration;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.benchmarks.common.BenchmarkType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author salman
 */
public class InterleavedBenchmarkCommand {

  public static class Request implements BenchmarkCommand.Request {
    private BMConfiguration config;

    public Request(BMConfiguration config) {
      this.config = config;
    }

    public BMConfiguration getConfig() {
      return config;
    }

    @Override
    public BenchmarkType getBenchMarkType() {
      return BenchmarkType.INTERLEAVED;
    }
  }

  public static class Response implements BenchmarkCommand.Response {

    private final long runTime;
    private final long totalSuccessfulOps;
    private final long totalFailedOps;
    private final double opsPerSec;
    private final double avgOpLatency;
    private final HashMap<BenchmarkOperations, ArrayList<Long>> opsExeTimes;
    private final List<String> failOverLog;
    private final int nnCount;

    public Response(long runTime, long totalSuccessfulOps, long totalFailedOps, double opsPerSec,
                    HashMap<BenchmarkOperations, ArrayList<Long>> opsExeTimes, double avgOpLatency, List<String> failOverLog,
                    int nnCount) {
      this.runTime = runTime;
      this.totalSuccessfulOps = totalSuccessfulOps;
      this.totalFailedOps = totalFailedOps;
      this.opsPerSec = opsPerSec;
      this.opsExeTimes = opsExeTimes;
      this.failOverLog = failOverLog;
      this.avgOpLatency = avgOpLatency;
      this.nnCount = nnCount;
    }

    public HashMap<BenchmarkOperations, ArrayList<Long>> getOpsExeTimes() {
      return opsExeTimes;
    }


    public long getRunTime() {
      return runTime;
    }

    public long getTotalSuccessfulOps() {
      return totalSuccessfulOps;
    }

    public long getTotalFailedOps() {
      return totalFailedOps;
    }

    public double getOpsPerSec() {
      return opsPerSec;
    }

    public List<String> getFailOverLog() {
      return failOverLog;
    }

    public double getAvgOpLatency() {
      return avgOpLatency;
    }

    public int getNnCount() {
      return nnCount;
    }
  }
}
