/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.experiments.benchmarks.interleaved;

import io.hops.experiments.benchmarks.OperationsUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import io.hops.experiments.benchmarks.common.NamespaceWarmUp;
import io.hops.experiments.coin.MultiFaceCoin;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.controller.commands.WarmUpCommand;
import io.hops.experiments.utils.BenchmarkUtils;
import io.hops.experiments.workload.generator.FilePool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import io.hops.experiments.benchmarks.common.Benchmark;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 * @author salman
 */
public class InterleavedBenchmark extends Benchmark {

  private long duration;
  private long startTime = 0;
  AtomicLong operationsCompleted = new AtomicLong(0);
  AtomicLong operationsFailed = new AtomicLong(0);
  Map<BenchmarkOperations, AtomicLong> operationsStats = new HashMap<BenchmarkOperations, AtomicLong>();
  HashMap<BenchmarkOperations, ArrayList<Long>> opsExeTimes = new HashMap<BenchmarkOperations, ArrayList<Long>>();
  private final int dirsPerDir;
  private final int filesPerDir;
  private final boolean fixedDepthTree;
  private final int treeDepth;

  public InterleavedBenchmark(Configuration conf, int numThreads, 
          int inodesPerDir, int filesPerDir,
          boolean fixedDepthTree, int treeDepth) {
    super(conf, numThreads);
    this.dirsPerDir = inodesPerDir;
    this.filesPerDir = filesPerDir;
    this.fixedDepthTree = fixedDepthTree;
    this.treeDepth = treeDepth;
  }

  @Override
  protected WarmUpCommand.Response warmUp(WarmUpCommand.Request warmUpCommand)
          throws IOException, InterruptedException {
    NamespaceWarmUp.Request namespaceWarmUp = (NamespaceWarmUp.Request) warmUpCommand;
    List workers = new ArrayList<BaseWarmUp>();
    for (int i = 0; i < numThreads; i++) {
      Callable worker = new BaseWarmUp(namespaceWarmUp.getFilesToCreate(),
              namespaceWarmUp.getReplicationFactor(), namespaceWarmUp
              .getFileSize(), namespaceWarmUp.getBaseDir(), 
              dirsPerDir, filesPerDir, fixedDepthTree, treeDepth);
      workers.add(worker);
    }
    executor.invokeAll(workers); // blocking call
    return new NamespaceWarmUp.Response();
  }

  @Override
  protected BenchmarkCommand.Response processCommandInternal(BenchmarkCommand.Request command) throws IOException, InterruptedException {
    InterleavedBenchmarkCommand.Request req = (InterleavedBenchmarkCommand.Request) command;

    duration = req.getDuration();
    System.out.println("Starting " + command.getBenchMarkType() + " for duration " + duration);
    List workers = new ArrayList<Worker>();
    for (int i = 0; i < numThreads; i++) {
      Callable worker = new Worker(req);
      workers.add(worker);
    }
    startTime = System.currentTimeMillis();
    executor.invokeAll(workers); // blocking call
    long totalTime = System.currentTimeMillis() - startTime;

    System.out.println("Finished " + command.getBenchMarkType() + " in " + totalTime);

    double speed = (operationsCompleted.get() / (double) totalTime) * 1000;

    InterleavedBenchmarkCommand.Response response =
            new InterleavedBenchmarkCommand.Response(totalTime, operationsCompleted.get(), operationsFailed.get(), speed, opsExeTimes);
    return response;
  }

  public class Worker implements Callable {

    private DistributedFileSystem dfs;
    private FilePool filePool;
    private InterleavedBenchmarkCommand.Request req;
    private MultiFaceCoin coin;

    public Worker(InterleavedBenchmarkCommand.Request req) throws IOException {
      this.req = req;
    }

    @Override
    public Object call() throws Exception {
      dfs = BenchmarkUtils.getDFSClient(conf);
      filePool = BenchmarkUtils.getFilePool(conf, req.getBaseDir(),
              dirsPerDir, filesPerDir, fixedDepthTree, treeDepth);
      coin = new MultiFaceCoin(req.getCreatePercent(), req.getAppendPercent(),
              req.getReadPercent(), req.getRenamePercent(), req.getDeletePercent(),
              req.getLsFilePercent(), req.getLsDirPercent(),
              req.getChmodFilePercent(), req.getChmodDirsPercent(), req.getMkdirPercent(),
              req.getSetReplicationPercent(), req.getFileInfoPercent(), req.getDirInfoPercent(),
              req.getFileChownPercent(), req.getDirChownPercent());
      String filePath = null;

      while (true) {
        try {
          if ((System.currentTimeMillis() - startTime) > duration) {
            return null;
          }

          BenchmarkOperations op = coin.flip();

          performOperation(op);

          log();

        } catch (Exception e) {
          Logger.error(e);
        }
      }
    }

    private void log() throws IOException {

      String message = "";
      if (Logger.canILog()) {

        message += format(25, "Completed Ops: " + operationsCompleted + " ");
        message += format(20, "Failed Ops: " + operationsFailed + " ");
        message += format(20, "Speed: " + speedPSec(operationsCompleted.get(), startTime) + " ops/s ");

//        SortedSet<BenchmarkOperations> sorted = new TreeSet<BenchmarkOperations>();
//        sorted.addAll(operationsStats.keySet());
//
//        for (BenchmarkOperations op : sorted) {
//          AtomicLong stat = operationsStats.get(op);
//          if (stat != null) {
//
//            double percent = BenchmarkUtils.round(((double) stat.get() / operationsCompleted.get()) * 100);
//            String msg = op + ": [" + percent + "%] ";
//            message += format(op.toString().length() + 14, msg);
//          }
//        }
        Logger.printMsg(message);
      }
    }

    private void performOperation(BenchmarkOperations opType) throws IOException {
      String path = OperationsUtils.getPath(opType, filePool);
      if (path != null) {
        boolean retVal = false;
        long opExeTime = 0;
        try {
          long opStartTime = 0L;
          opStartTime = System.currentTimeMillis();

          OperationsUtils.performOp(dfs, opType, filePool, path, req.getReplicationFactor(), req.getFileSize(), req.getAppendSize());
          opExeTime = System.currentTimeMillis() - opStartTime;
          retVal = true;
        } catch (Exception e) {
          Logger.error(e);
        }
        updateStats(opType, retVal, opExeTime);
      } else {
        Logger.printMsg("Could not perform operation " + opType + ". Got Null from the file pool");
      }
    }

    private void updateStats(BenchmarkOperations opType, boolean success, long opExeTime) {
      AtomicLong stat = operationsStats.get(opType);
      if (stat == null) { // this should be synchronized to get accurate stats. However, this will slow down and these stats are just for log messages. Some inconsistencies are OK
        stat = new AtomicLong(0);
        operationsStats.put(opType, stat);
      }
      stat.incrementAndGet();

      if (success) {
        operationsCompleted.incrementAndGet();
        if (req.isPercentileEnabled()) {
          synchronized (opsExeTimes) {
            ArrayList<Long> times = opsExeTimes.get(opType);
            if (times == null) {
              times = new ArrayList<Long>();
              opsExeTimes.put(opType, times);
            }
            times.add(opExeTime);
          }
        }
      } else {
        operationsFailed.incrementAndGet();
      }

    }
  }

  protected String format(int spaces, String string) {
    String format = "%1$-" + spaces + "s";
    return String.format(format, string);
  }

  private double speedPSec(long ops, long startTime) {
    long timePassed = (System.currentTimeMillis() - startTime);
    double opsPerMSec = (double) (ops) / (double) timePassed;
    return BenchmarkUtils.round(opsPerMSec * 1000);
  }
}
