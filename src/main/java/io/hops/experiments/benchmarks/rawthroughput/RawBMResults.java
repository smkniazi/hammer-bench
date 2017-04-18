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
package io.hops.experiments.benchmarks.rawthroughput;

import io.hops.experiments.benchmarks.common.BMResult;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.common.BenchmarkType;
import io.hops.experiments.utils.DFSOperationsUtils;

/**
 *
 * @author salman
 */
public class RawBMResults extends BMResult {
  private final double speed;
  private final double duration;
  private final double successfulOps;
  private final double failedOps;
  private final BenchmarkOperations operationType;

  public RawBMResults(int noOfExpectedNNs, int noOfActualAliveNNs, int noOfNDBDataNodes, BenchmarkOperations operationType, double speed, double duration, double successfulOps, double failedOps) {
    super(noOfExpectedNNs, noOfActualAliveNNs, noOfNDBDataNodes, BenchmarkType.RAW);
    this.speed = speed;
    this.duration = duration;
    this.successfulOps = successfulOps;
    this.failedOps = failedOps;
    this.operationType = operationType;
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

  public BenchmarkOperations getOperationType() {
    return operationType;
  }

  @Override
  public String toString() {
    String message = operationType +" " 
            + DFSOperationsUtils.round(speed) + " ops/sec. " +
                " Successful-Ops: " + DFSOperationsUtils.round(successfulOps)
            + " Failed-Ops: " + DFSOperationsUtils.round(failedOps)
            + " Avg-Test-Duration-sec " + DFSOperationsUtils.round(duration)
            + " No of Expected NNs "+super.getNoOfExpectedAliveNNs()
            + " No of Actual Alive NNs "+super.getNoOfAcutallAliveNNs();
    return message;
  }

  
  
}
