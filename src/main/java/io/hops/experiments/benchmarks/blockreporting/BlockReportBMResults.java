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

import io.hops.experiments.benchmarks.BMResult;
import io.hops.experiments.benchmarks.common.BenchmarkType;
import io.hops.experiments.utils.BenchmarkUtils;
import java.text.DecimalFormat;

/**
 *
 * @author salman
 */
public class BlockReportBMResults extends BMResult {
  private final double speed;
  private final double successfulOps;
  private final double failedOps;
  private final double avgTimePerReport;
  private final double avgTimeToGetNameNodeToReport;


  public BlockReportBMResults(int noOfNameNodes, int noOfNDBDataNodes, double speed, double successfulOps, double failedOps,
          double avgTimePerReport, double avgTimeToGetNameNodeToReport) {
    super(noOfNameNodes,noOfNDBDataNodes, BenchmarkType.INTERLEAVED);
    this.speed = speed;
    this.successfulOps = successfulOps;
    this.failedOps = failedOps;
    this.avgTimeToGetNameNodeToReport =avgTimeToGetNameNodeToReport;
    this.avgTimePerReport = avgTimePerReport;
  }

  public double getSpeed() {
    return speed;
  }
  
  public double getSuccessfulOps() {
    return successfulOps;
  }

  public double getFailedOps() {
    return failedOps;
  }

  public double getAvgTimePerReport() {
    return avgTimePerReport;
  }

  public double getAvgTimeToGetNameNodeToReport() {
    return avgTimeToGetNameNodeToReport;
  }

  @Override
  public String toString() {
     String message = "Successful-Ops: " + BenchmarkUtils.round(successfulOps)
                + " Failed-Ops: " + BenchmarkUtils.round(failedOps)
                + " Speed-/sec: " + BenchmarkUtils.round(speed)
                + " AvgTimePerReport: " + BenchmarkUtils.round(avgTimePerReport)
                + " AvgTimeToGetNameNodeToReport: " + BenchmarkUtils.round(avgTimeToGetNameNodeToReport)
                + " No of NameNodes: "+super.getNoOfNamenodes();
    return message;
  }
}
