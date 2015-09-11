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

import com.google.common.collect.Lists;
import io.hops.experiments.benchmarks.common.Benchmark;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.controller.commands.WarmUpCommand;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;


import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockReportingBenchmark extends Benchmark{

  private static final Random rand = new Random(UUID.randomUUID().getLeastSignificantBits());

  private long startTime;
  private AtomicInteger successfulOps = new AtomicInteger(0);
  private AtomicInteger failedOps = new AtomicInteger(0);

  private DescriptiveStatistics getNewNameNodeElapsedTime = new
      DescriptiveStatistics();
  private DescriptiveStatistics brElapsedTimes = new DescriptiveStatistics();

  private TinyDatanodes datanodes;
  private ExecutorService executor;
  private int slaveId;
  public BlockReportingBenchmark(Configuration conf, int numThreads, int
      slaveID) {
    super(conf, numThreads);
    this.slaveId = slaveID;
    this.executor = Executors.newFixedThreadPool(numThreads);
  }

  @Override
  protected WarmUpCommand.Response warmUp(WarmUpCommand.Request warmUp)
      throws IOException, InterruptedException {
    BlockReportingWarmUp.Request request =
        (BlockReportingWarmUp.Request) warmUp;

    datanodes = new TinyDatanodes(conf, request.getBaseDir(), numThreads,
        request.getBlocksPerReport(), request.getBlocksPerFile(), request.getFilesPerDir(),
        request.getReplication(), request.getMaxBlockSize(), slaveId, request
        .getDatabaseConnection());

    try {
      long t = Time.now();
      datanodes.generateInput(request.isSkipCreations(), executor);
      Logger.printMsg("WarmUp done in " + (Time.now() - t) /1000 + " seconds");
    }catch (Exception e){
      e.printStackTrace();
      Logger.error(e);
    }

    return new BlockReportingWarmUp.Response();
  }

  @Override
  protected BenchmarkCommand.Response processCommandInternal(
      BenchmarkCommand.Request command)
      throws IOException, InterruptedException {
    BlockReportingBenchmarkCommand.Request request =
        (BlockReportingBenchmarkCommand.Request) command;
    int numOfReports = request.getNumOfReports();

    List workers = Lists.newArrayList();
    for(int dn = 0; dn < numThreads; dn++){
      workers.add(new Reporter(dn, numOfReports, request
          .getMinTimeBeforeNextReport(), request.getMaxTimeBeforeNextReport()));
    }

    startTime = Time.now();
    executor.invokeAll(workers);
    double speed = currentSpeed();

    datanodes.printStats();

    return new BlockReportingBenchmarkCommand.Response(successfulOps.get(),
        failedOps.get(), speed ,brElapsedTimes.getMean(),
        getNewNameNodeElapsedTime.getMean());
  }

  private class Reporter implements Callable{
    private final int dnIdx;
    private final int numOfReports;
    private final int minTimeBeforeNextReport;
    private final int maxTimeBeforeNextReport;

    public Reporter(int dnIdx, int numOfReports, int minTimeBeforeNextReport,
        int maxTimeBeforeNextReport) {
      this.dnIdx = dnIdx;
      this.numOfReports = numOfReports;
      this.minTimeBeforeNextReport = minTimeBeforeNextReport;
      this.maxTimeBeforeNextReport = maxTimeBeforeNextReport;
    }

    @Override
    public Object call() throws Exception {
      for(int report = 0; report < numOfReports; report++) {
        try {

          if(minTimeBeforeNextReport > 0 && maxTimeBeforeNextReport > 0) {
            long sleep = minTimeBeforeNextReport + rand.nextInt
                (maxTimeBeforeNextReport - minTimeBeforeNextReport);
            Thread.sleep(sleep);
          }

          long[] ts = datanodes.executeOp(dnIdx);
          successfulOps.incrementAndGet();
          getNewNameNodeElapsedTime.addValue(ts[0]);
          brElapsedTimes.addValue(ts[1]);

          if(Logger.canILog()){
            Logger.printMsg("Successful BR ops " + successfulOps.get
                () + " Failed BR ops " + failedOps.get() + " Speed " +
                currentSpeed() + " " +
                "ops/sec" + " BR details [nn=" + ts[0] + "(" +
                getNewNameNodeElapsedTime.getMean() +"), br=" +
                ts[1]+ "(" + brElapsedTimes.getMean() + ")]");
          }

        }catch (Exception e){
          failedOps.incrementAndGet();
          Logger.error(e);
        }
      }
      return null;
    }
  }

  double currentSpeed(){
    double timePassed = Time.now() - startTime;
    double opsPerMSec = (double) (successfulOps.get()) / timePassed;
    return opsPerMSec * 1000;
  }
}
