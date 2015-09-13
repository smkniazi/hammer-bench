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
import io.hops.experiments.benchmarks.common.BenchmarkType;
import io.hops.experiments.utils.BenchmarkUtils;
import java.text.DecimalFormat;

/**
 *
 * @author salman
 */
public class InterleavedBMResults extends BMResult {
  private final double speed;
  private final double duration;
  private final double successfulOps;
  private final double failedOps;

  public InterleavedBMResults(int noOfNameNodes, int noOfNDBDataNodes, double speed, double duration, double successfulOps, double failedOps) {
    super(noOfNameNodes, noOfNDBDataNodes, BenchmarkType.INTERLEAVED);
    this.speed = speed;
    this.duration = duration;
    this.successfulOps = successfulOps;
    this.failedOps = failedOps;
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

  @Override
  public String toString() {

    String message = "Speed-/sec: " + BenchmarkUtils.round(speed) 
            + " Successful-Ops: " + BenchmarkUtils.round(successfulOps)
            + " Failed-Ops: " + BenchmarkUtils.round(failedOps)
            + " Avg-Test-Duration-sec " + BenchmarkUtils.round(duration)
            + " No of NameNodes: "+super.getNoOfNamenodes();

    return message;
  }
}
