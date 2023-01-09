/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.experiments.benchmarks.rawthroughput;

import io.hops.experiments.benchmarks.common.config.BMConfiguration;
import io.hops.experiments.utils.BMOperationsUtils;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
import io.hops.experiments.benchmarks.common.coin.FileSizeMultiFaceCoin;
import io.hops.experiments.benchmarks.common.commands.NamespaceWarmUp;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.controller.commands.WarmUpCommand;
import io.hops.experiments.utils.DFSOperationsUtils;
import org.apache.hadoop.conf.Configuration;
import io.hops.experiments.benchmarks.common.Benchmark;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.workload.generator.FilePool;
import org.apache.hadoop.fs.FileSystem;

/**
 * @author salman
 */
public class RawBenchmark extends Benchmark {

  private AtomicInteger successfulOps = new AtomicInteger(0);
  private AtomicInteger failedOps = new AtomicInteger(0);
  private long phaseStartTime;
  private long phaseDurationInMS;
  private final ArrayList<Long> opsExeTimes = new ArrayList<Long>();

  public RawBenchmark(Configuration conf, BMConfiguration bmConf) {
    super(conf, bmConf);
  }

  @Override
  protected WarmUpCommand.Response warmUp(WarmUpCommand.Request cmd)
          throws IOException, InterruptedException {
    // Warn up is done in two stages.
    // In the first phase all the parent dirs are created
    // and then in the second stage we create the further
    // file/dir in the parent dir.

    if (bmConf.getFilesToCreateInWarmUpPhase() > 1) {
      List workers = new ArrayList<BaseWarmUp>();
      // Stage 1
      threadsWarmedUp.set(0);
      for (int i = 0; i < bmConf.getSlaveNumThreads(); i++) {
        Callable worker = new BaseWarmUp(1, bmConf, "Warming up. Stage1: Creating Parent Dirs. ");
        workers.add(worker);
      }
      executor.invokeAll(workers); // blocking call
      workers.clear();

      // Stage 2
      threadsWarmedUp.set(0);
      for (int i = 0; i < bmConf.getSlaveNumThreads(); i++) {
        Callable worker = new BaseWarmUp(bmConf.getFilesToCreateInWarmUpPhase() - 1, bmConf,
                "Warming up. Stage2: Creating files/dirs. ");
        workers.add(worker);
      }
      executor.invokeAll(workers); // blocking call
      Logger.printMsg("Finished. Warmup Phase. Created (" + bmConf.getSlaveNumThreads() + "*" + bmConf.getFilesToCreateInWarmUpPhase() + ") = " +
              (bmConf.getSlaveNumThreads() * bmConf.getFilesToCreateInWarmUpPhase()) + " files. ");
      workers.clear();
    }
    return new NamespaceWarmUp.Response();
  }

  @Override
  protected BenchmarkCommand.Response processCommandInternal(BenchmarkCommand.Request command)
          throws IOException, InterruptedException {
    RawBenchmarkCommand.Request request = (RawBenchmarkCommand.Request) command;
    RawBenchmarkCommand.Response response;
    System.out.println("Starting the " + request.getPhase() + " duration " + request.getDurationInMS());
    response = startTestPhase(request.getPhase(), request.getDurationInMS(), bmConf.getBaseDir());
    return response;
  }

  private RawBenchmarkCommand.Response startTestPhase(BenchmarkOperations opType, long duration, String baseDir) throws InterruptedException, UnknownHostException, IOException {
    List workers = new LinkedList<Callable>();
    for (int i = 0; i < bmConf.getSlaveNumThreads(); i++) {
      Callable worker = new Generic(baseDir, opType);
      workers.add(worker);
    }
    setMeasurementVariables(duration);

    Logger.resetTimer();

    executor.invokeAll(workers);// blocking call
    long phaseFinishTime = System.currentTimeMillis();
    long actualExecutionTime = (phaseFinishTime - phaseStartTime);

    double speed = ((double) successfulOps.get() / (double) actualExecutionTime); // p / ms
    speed = speed * 1000;

    RawBenchmarkCommand.Response response =
            new RawBenchmarkCommand.Response(opType,
                    actualExecutionTime, successfulOps.get(), failedOps.get(), speed, getAliveNNsCount(), opsExeTimes);
    return response;
  }

  public class Generic implements Callable {

    private BenchmarkOperations opType;
    private FileSystem dfs;
    private FilePool filePool;
    private String baseDir;
    private long lastLog = System.currentTimeMillis();

    public Generic(String baseDir, BenchmarkOperations opType) throws IOException {
      this.baseDir = baseDir;
      this.opType = opType;
    }

    Map<Long, Long> stats = new HashMap<Long, Long>();

    @Override
    public Object call() throws Exception {
      try {
        dfs = DFSOperationsUtils.getDFSClient(conf);
        filePool = DFSOperationsUtils.getFilePool(conf, bmConf.getBaseDir(),
                bmConf.getDirPerDir(), bmConf.getFilesPerDir(), bmConf.isFixedDepthTree(),
                bmConf.getTreeDepth(), bmConf.getFileSizeDistribution(),
                bmConf.getReadFilesFromDisk(), bmConf.getDiskNameSpacePath());
      } catch (Exception e) {
        Logger.error(e);
        e.printStackTrace();
        throw e;
      }
      while (true) {
        try {

          String path = BMOperationsUtils.getPath(opType, filePool);

          if (path == null) {
            return null;
          } else if ((System.currentTimeMillis() - phaseStartTime) > phaseDurationInMS) {
            return null;
          } else if (opType == BenchmarkOperations.CREATE_FILE &&
                  bmConf.getRawBmMaxFilesToCreate() < (long) (successfulOps.get() + filesCreatedInWarmupPhase.get())) {
            return null;
          } else if (opType == BenchmarkOperations.CREATE_FILE &&
                  bmConf.getReadFilesFromDisk() && !filePool.hasMoreFilesToWrite()) {
            return null;
          }

          long fileSize = -1;
          if (opType == opType.CREATE_FILE) {
            /*For logging file size distribution
            synchronized (this) {
              Long count = stats.get(fileSize);
              Long newCount = count == null ? 1 : count + 1;
              stats.put(fileSize, newCount);
            }*/
          }

          long time = 0;
          if (bmConf.isPercentileEnabled()) {
            time = System.nanoTime();
          }
          BMOperationsUtils.performOp(dfs, opType, filePool, path, bmConf.getReplicationFactor(),
                  bmConf.getAppendFileSize());
          if (bmConf.isPercentileEnabled()) {
            time = System.nanoTime() - time;
          }
          logStats(opType, time);

          logMessage();

        } catch (Throwable e) {
          failedOps.incrementAndGet();
          Logger.error(e);
        }
      }
    }

    private void logStats(BenchmarkOperations type, long time) {
      if (bmConf.isPercentileEnabled()) {
        synchronized (opsExeTimes) {
          opsExeTimes.add(time);
        }
      }
      successfulOps.incrementAndGet();
    }

    private void logMessage() {
      // Send a log message once every five second.
      // The logger also tires to rate limit the log messages
      // using the canILog() methods. canILog method is synchronized
      // method. Calling it frequently can slightly impact the performance
      // It is better that each thread call the canILog() method only
      // once every five sec
      if ((System.currentTimeMillis() - lastLog) > 5000) {
        lastLog = System.currentTimeMillis();
        if (Logger.canILog()) {
          Logger.printMsg("Successful " + opType + " ops " + successfulOps.get() + " Failed ops " + failedOps.get() + " Speed: " + DFSOperationsUtils.round(speedPSec(successfulOps, phaseStartTime)));

          //log file size distribution
          /*synchronized (this) {
            String msg = "";
            Long total = new Long(0);
            for (Long size : stats.keySet()) {
              Long count = stats.get(size);
              total += count;
            }
            for (Long size : stats.keySet()) {
              Long count = stats.get(size);
              msg += "  Size: " + size + " Percentage: " + (count / (double) total) * 100+"\n";
            }
            Logger.printMsg("Ratio: " + msg);
          }*/
        }
      }
    }
  }

  private void setMeasurementVariables(long duration) {
    phaseDurationInMS = duration;
    phaseStartTime = System.currentTimeMillis();
    successfulOps = new AtomicInteger(0);
    failedOps = new AtomicInteger(0);
    opsExeTimes.clear();
  }

  public double speedPSec(AtomicInteger ops, long startTime) {
    long timePassed = (System.currentTimeMillis() - startTime);
    double opsPerMSec = (double) (ops.get()) / (double) timePassed;
    return opsPerMSec * 1000;
  }
}
