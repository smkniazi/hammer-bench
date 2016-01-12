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
package io.hops.experiments.benchmarks.interleaved;

import io.hops.experiments.benchmarks.BMResult;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.common.BenchmarkType;
import io.hops.experiments.utils.BenchmarkUtils;
import java.text.DecimalFormat;
import java.util.Map;

/**
 *
 * @author salman
 */
public class InterleavedBMResults extends BMResult {
  private final double speed;
  private final double duration;
  private final double successfulOps;
  private final double failedOps;
  private final Map<BenchmarkOperations,double[][]> percentile;
  private final String workloadName;
  private final double avgOpLatency;

  public InterleavedBMResults(int noOfNameNodes, int noOfNDBDataNodes, String workloadName, double speed, double duration, double successfulOps, double failedOps, Map<BenchmarkOperations,double[][]> percentile,double avgOpLatency) {
    super(noOfNameNodes, noOfNDBDataNodes, BenchmarkType.INTERLEAVED);
    this.speed = speed;
    this.duration = duration;
    this.successfulOps = successfulOps;
    this.failedOps = failedOps;
    this.percentile = percentile;
    this.workloadName = workloadName;
    this.avgOpLatency = avgOpLatency;
  }

  public String getWorkloadName() {
    return workloadName;
  }

  public Map<BenchmarkOperations,double[][]> getPercentile(){
    return percentile;
  }
  
  public double getSpeed() {
    return speed;
  }

  public double getDuration() {
    return duration;
  }

  public double getSuccessfulOps() {
    return successfulOps;
  }

  public double getFailedOps() {
    return failedOps;
  }

  public double getAvgOpLatency() {
    return avgOpLatency;
  }

  @Override
  public String toString() {

    String message = "Speed-/sec: " + BenchmarkUtils.round(speed) 
            + " Successful-Ops: " + BenchmarkUtils.round(successfulOps)
            + " Failed-Ops: " + BenchmarkUtils.round(failedOps)
            + " Avg-Ops-Latency: " + BenchmarkUtils.round(avgOpLatency)
            + " Avg-Test-Duration-sec " + BenchmarkUtils.round(duration)
            + " No of NameNodes: "+super.getNoOfNamenodes();

    return message;
  }
}
