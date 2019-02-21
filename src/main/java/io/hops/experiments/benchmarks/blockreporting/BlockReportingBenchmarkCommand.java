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
import io.hops.experiments.controller.commands.BenchmarkCommand;

public class BlockReportingBenchmarkCommand {

  public static class Request implements BenchmarkCommand.Request{

    public Request() {
    }

    @Override
    public BenchmarkType getBenchMarkType() {
      return BenchmarkType.BR;
    }
  }

  public static class Response implements BenchmarkCommand.Response{
    private final int successfulOps;
    private final int failedOps;
    private final double speed;
    private final double avgTimePerReport;
    private final double avgTimeTogetNewNameNode;
    private final int nnCount;

    public Response(int successfulOps, int failedOps, double speed,
        double avgTimePerReport, double avgTimeTogetNewNameNode, int nnCount) {
      this.successfulOps = successfulOps;
      this.failedOps = failedOps;
      this.speed = speed;
      this.avgTimePerReport = avgTimePerReport;
      this.avgTimeTogetNewNameNode = avgTimeTogetNewNameNode;
      this.nnCount = nnCount;
    }

    public int getSuccessfulOps() {
      return successfulOps;
    }

    public int getFailedOps() {
      return failedOps;
    }

    public double getSpeed() {
      return speed;
    }

    public double getAvgTimePerReport() {
      return avgTimePerReport;
    }

    public double getAvgTimeTogetNewNameNode() {
      return avgTimeTogetNewNameNode;
    }

    public int getNnCount() {
      return nnCount;
    }
  }
}
