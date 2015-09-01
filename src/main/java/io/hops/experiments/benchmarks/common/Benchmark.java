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
package io.hops.experiments.benchmarks.common;

import io.hops.experiments.benchmarks.blockreporting.BlockReportingBenchmark;
import io.hops.experiments.benchmarks.interleaved.InterleavedBenchmark;
import io.hops.experiments.benchmarks.rawthroughput.RawBenchmark;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.controller.commands.WarmUpCommand;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public abstract class Benchmark {

  protected final Configuration conf;
  protected final int numThreads;

  public Benchmark(Configuration conf, int numThreads) {
    this.conf = conf;
    this.numThreads = numThreads;
  }

  protected abstract WarmUpCommand.Response warmUp(WarmUpCommand.Request warmUp)
      throws IOException, InterruptedException;

  protected abstract BenchmarkCommand.Response processCommandInternal
      (BenchmarkCommand.Request command)  throws IOException,
      InterruptedException;

  public final BenchmarkCommand.Response processCommand(BenchmarkCommand
      .Request command)
      throws IOException, InterruptedException{
    if(command instanceof WarmUpCommand.Request){
      return warmUp((WarmUpCommand.Request) command);
    }
    return processCommandInternal(command);
  }

  public static Benchmark getBenchmark(BenchmarkType type, int numThreads,
      Configuration conf, int slaveId, int inodesPerDir){
    if(type == BenchmarkType.RAW){
      return new RawBenchmark(conf, numThreads, inodesPerDir);
    }else if(type == BenchmarkType.INTERLEAVED){
      return new InterleavedBenchmark(conf, numThreads, inodesPerDir);
    }else if(type == BenchmarkType.BR){
      return new BlockReportingBenchmark(conf, numThreads, slaveId);
    }else {
      throw new UnsupportedOperationException("Unsupported Benchmark " + type);
    }
  }
}
