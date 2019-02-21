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

import io.hops.experiments.benchmarks.common.config.BMConfiguration;
import io.hops.experiments.utils.BMOperationsUtils;
import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
import io.hops.experiments.benchmarks.common.Benchmark;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.common.commands.NamespaceWarmUp;
import io.hops.experiments.benchmarks.interleaved.coin.InterleavedMultiFaceCoin;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.controller.commands.WarmUpCommand;
import io.hops.experiments.utils.DFSOperationsUtils;
import io.hops.experiments.workload.generator.FilePool;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author salman
 */
public class InterleavedBenchmark extends Benchmark {

  private long duration;
  private long startTime = 0;
  AtomicLong operationsCompleted = new AtomicLong(0);
  AtomicLong operationsFailed = new AtomicLong(0);
  Map<BenchmarkOperations, AtomicLong> operationsStats = new HashMap<BenchmarkOperations, AtomicLong>();
  HashMap<BenchmarkOperations, ArrayList<Long>> opsExeTimes = new HashMap<BenchmarkOperations, ArrayList<Long>>();
  SynchronizedDescriptiveStatistics avgLatency = new SynchronizedDescriptiveStatistics();
  private String fileSizeDistribution;
  private final int dirsPerDir;
  private final int filesPerDir;
  private boolean readFilesFromDisk;
  private String diskFilesPath;
  private final boolean percentileEnabled;


  private final boolean fixedDepthTree;
  private final int treeDepth;

  public InterleavedBenchmark(Configuration conf, int numThreads,
                              int inodesPerDir, int filesPerDir,
                              boolean fixedDepthTree, int treeDepth,
                              boolean percentileEnabled,
                              BenchMarkFileSystemName fsName) {
    super(conf, numThreads, fsName);
    this.dirsPerDir = inodesPerDir;
    this.filesPerDir = filesPerDir;
    this.fixedDepthTree = fixedDepthTree;
    this.treeDepth = treeDepth;
    this.percentileEnabled = percentileEnabled;
  }

  @Override
  protected WarmUpCommand.Response warmUp(WarmUpCommand.Request warmUpCommand)
          throws IOException, InterruptedException {
    NamespaceWarmUp.Request namespaceWarmUp = (NamespaceWarmUp.Request) warmUpCommand;
    fileSizeDistribution = namespaceWarmUp.getFileSizeDistribution();
    readFilesFromDisk = namespaceWarmUp.isReadFilesFromDisk();
    diskFilesPath = namespaceWarmUp.getDiskFilsPath();
    // Warn up is done in two stages.
    // In the first phase all the parent dirs are created
    // and then in the second stage we create the further
    // file/dir in the parent dir.

    if (namespaceWarmUp.getFilesToCreate() > 1) {
      List workers = new ArrayList<BaseWarmUp>();
      // Stage 1
      threadsWarmedUp.set(0);
      for (int i = 0; i < numThreads; i++) {
        Callable worker = new BaseWarmUp(1,
                namespaceWarmUp.getReplicationFactor(), namespaceWarmUp
                .getFileSizeDistribution(), namespaceWarmUp.getBaseDir(),
                dirsPerDir, filesPerDir, fixedDepthTree, treeDepth,
                readFilesFromDisk, diskFilesPath, "Warming up. Stage1: Creating Parent Dirs. ");
        workers.add(worker);
      }
      executor.invokeAll(workers); // blocking call
      workers.clear();

      // Stage 2
      threadsWarmedUp.set(0);
      for (int i = 0; i < numThreads; i++) {
        Callable worker = new BaseWarmUp(namespaceWarmUp.getFilesToCreate() - 1,
                namespaceWarmUp.getReplicationFactor(), namespaceWarmUp
                .getFileSizeDistribution(), namespaceWarmUp.getBaseDir(),
                dirsPerDir, filesPerDir, fixedDepthTree, treeDepth,
                readFilesFromDisk, diskFilesPath, "Warming up. Stage2: Creating files/dirs. ");
        workers.add(worker);
      }
      executor.invokeAll(workers); // blocking call
      Logger.printMsg("Finished. Warmup Phase. Created ("+numThreads+"*"+namespaceWarmUp.getFilesToCreate()+") = "+
              (numThreads*namespaceWarmUp.getFilesToCreate())+" files. ");
      workers.clear();
    }

    return new NamespaceWarmUp.Response();
  }

  @Override
  protected BenchmarkCommand.Response processCommandInternal(BenchmarkCommand.Request command) throws IOException, InterruptedException {
    BMConfiguration config = ((InterleavedBenchmarkCommand.Request) command).getConfig();

    duration = config.getInterleavedBmDuration();
    System.out.println("Starting " + command.getBenchMarkType() + " for duration " + duration);
    List workers = new ArrayList<Worker>();
    for (int i = 0; i < numThreads; i++) {
      Callable worker = new Worker(config);
      workers.add(worker);
    }
    startTime = System.currentTimeMillis();

    FailOverMonitor failOverTester = null;
    List<String> failOverLog = null;
    if (config.testFailover()) {
      boolean canIKillNamenodes = InetAddress.getLocalHost().getHostName().compareTo(config.getNamenodeKillerHost()) == 0;
      if (canIKillNamenodes) {
        Logger.printMsg("Responsible for killing/restarting namenodes");
      }
      failOverTester = startFailoverTestDeamon(
              config.getNameNodeRestartCommands(),
              config.getFailOverTestDuration(),
              config.getFailOverTestStartTime(),
              config.getNameNodeRestartTimePeriod(),
              canIKillNamenodes);
    }

    Logger.resetTimer();

    executor.invokeAll(workers); // blocking call
    if (config.testFailover()) {
      failOverTester.stop();
      failOverLog = failOverTester.getFailoverLog();
    }

    long totalTime = System.currentTimeMillis() - startTime;


    System.out.println("Finished " + command.getBenchMarkType() + " in " + totalTime);

    double speed = (operationsCompleted.get() / (double) totalTime) * 1000;

    InterleavedBenchmarkCommand.Response response =
            new InterleavedBenchmarkCommand.Response(totalTime, operationsCompleted.get(), operationsFailed.get(), speed, opsExeTimes, avgLatency.getMean(), failOverLog, getAliveNNsCount());
    return response;
  }

  public class Worker implements Callable {

    private FileSystem dfs;
    private FilePool filePool;
    private InterleavedMultiFaceCoin opCoin;
    private BMConfiguration config = null;
    private long lastMsg = System.currentTimeMillis();

    public Worker(BMConfiguration config) throws IOException {
      this.config = config;
      this.lastMsg = System.currentTimeMillis();
    }

    @Override
    public Object call() throws Exception {
      dfs = DFSOperationsUtils.getDFSClient(conf);
      filePool = DFSOperationsUtils.getFilePool(conf, config.getBaseDir(),
              dirsPerDir, filesPerDir, fixedDepthTree, treeDepth,
              fileSizeDistribution, readFilesFromDisk, diskFilesPath);
      opCoin = new InterleavedMultiFaceCoin(config.getInterleavedBmCreateFilesPercentage(),
              config.getInterleavedBmAppendFilePercentage(),
              config.getInterleavedBmReadFilesPercentage(),
              config.getInterleavedBmRenameFilesPercentage(),
              config.getInterleavedBmDeleteFilesPercentage(),
              config.getInterleavedBmLsFilePercentage(),
              config.getInterleavedBmLsDirPercentage(),
              config.getInterleavedBmChmodFilesPercentage(),
              config.getInterleavedBmChmodDirsPercentage(),
              config.getInterleavedBmMkdirPercentage(),
              config.getInterleavedBmSetReplicationPercentage(),
              config.getInterleavedBmGetFileInfoPercentage(),
              config.getInterleavedBmGetDirInfoPercentage(),
              config.getInterleavedBmFileChangeOwnerPercentage(),
              config.getInterleavedBmDirChangeOwnerPercentage()
      );
      while (true) {
        try {
          if ((System.currentTimeMillis() - startTime) > duration) {
            return null;
          }

          BenchmarkOperations op = opCoin.flip();

          performOperation(op);

          if (!config.testFailover()) {
            log();
          }

        } catch (Exception e) {
          Logger.error(e);
        }
      }
    }


    private void log() throws IOException {

      // Send a log message once every five second.
      // The logger also tires to rate limit the log messages
      // using the canILog() methods. canILog method is synchronized
      // method. Calling it frequently can slightly impact the performance
      // It is better that each thread call the canILog() method only
      // once every five sec
      if ((System.currentTimeMillis() - lastMsg) > 5000) {
        lastMsg = System.currentTimeMillis();
        String message = "";
        if (Logger.canILog()) {
          message += DFSOperationsUtils.format(25, "Completed Ops: " + operationsCompleted + " ");
          message += DFSOperationsUtils.format(25, "Failed Ops: " + operationsFailed + " ");
          message += DFSOperationsUtils.format(25, "Speed: " + DFSOperationsUtils.round(speedPSec(operationsCompleted.get(), startTime)));
//          if (avgLatency.getN() > 0) {
//            message += DFSOperationsUtils.format(20, "Avg. Op Latency: " + avgLatency.getMean() + " ms");
//          }
//
//          SortedSet<BenchmarkOperations> sorted = new TreeSet<BenchmarkOperations>();
//          sorted.addAll(operationsStats.keySet());
//
//          for (BenchmarkOperations op : sorted) {
//            AtomicLong stat = operationsStats.get(op);
//            if (stat != null) {
//
//              double percent = DFSOperationsUtils.round(((double) stat.get() / operationsCompleted.get()) * 100);
//              String msg = op + ": [" + percent + "%] ";
//              message += DFSOperationsUtils.format(op.toString().length() + 14, msg);
//            }
//          }
          Logger.printMsg(message);
        }
      }
    }

    private void performOperation(BenchmarkOperations opType) throws IOException {
      String path = BMOperationsUtils.getPath(opType, filePool);
      if (path != null) {
        boolean retVal = false;
        long opExeTime = 0;
        try {
          long opStartTime = 0L;
          opStartTime = System.nanoTime();

          BMOperationsUtils.performOp(dfs, opType, filePool, path, config.getReplicationFactor(),
                  config.getAppendFileSize());
          opExeTime = System.nanoTime() - opStartTime;
          retVal = true;
        } catch (Exception e) {
          Logger.error(e);
        }
        updateStats(opType, retVal, opExeTime);
      } else {
        Logger.printMsg("Could not perform operation " + opType + ". Got Null from the file pool");
//                System.exit(-1);
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
        avgLatency.addValue(opExeTime);
        if (percentileEnabled) {
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

  private double speedPSec(long ops, long startTime) {
    long timePassed = (System.currentTimeMillis() - startTime);
    double opsPerMSec = (double) (ops) / (double) timePassed;
    return opsPerMSec * 1000;
  }

  FailOverMonitor startFailoverTestDeamon(List<List<String>> commands, long failoverTestDuration, long failoverTestStartTime, long namenodeRestartTP, boolean canIKillNamenodes) {
    FailOverMonitor worker = new FailOverMonitor(commands, failoverTestDuration, failoverTestStartTime, namenodeRestartTP, canIKillNamenodes);
    Thread t = new Thread(worker);
    t.start();
    return worker;
  }

  class FailOverMonitor implements Runnable {
    boolean stop;
    List<List<String>> allCommands;
    List<String> log;
    int tick = 0;
    long namenodeRestartTP;
    long failoverTestDuration;
    long failoverStartTime;
    boolean canIKillNNs;

    public FailOverMonitor(List<List<String>> commands,
                           long failoverTestDuration, long failoverTestStartTime,
                           long namenodeRestartTP, boolean canIKillNNs) {
      this.allCommands = commands;
      this.namenodeRestartTP = namenodeRestartTP;
      this.stop = false;
      this.failoverStartTime = failoverTestStartTime;
      this.failoverTestDuration = failoverTestDuration;
      this.log = new LinkedList<String>();
      this.canIKillNNs = canIKillNNs;
    }

    @Override
    public void run() {
      int rrIndex = 0;
      long previousSuccessfulOps = 0;
      final long startTime = System.currentTimeMillis();
      long lastFailOver = 0;
      while (!stop) {
        long speed = 0;
        if (previousSuccessfulOps == 0) {
          speed = operationsCompleted.get();
          previousSuccessfulOps = speed;
        } else {
          speed = (operationsCompleted.get() - previousSuccessfulOps);
          previousSuccessfulOps = operationsCompleted.get();
        }

        log.add(tick + " " + speed);
        Logger.printMsg("Time: " + tick + " sec. Speed: " + speed);


        if (canIKillNNs) {
          if (((System.currentTimeMillis() - startTime) > failoverStartTime)) {
            if ((startTime + failoverStartTime + failoverTestDuration) > System.currentTimeMillis()) {
              if (System.currentTimeMillis() - lastFailOver > namenodeRestartTP) {
                int index = (rrIndex++) % allCommands.size();
                List<String> nnCommands = allCommands.get(index);
                new Thread(new FailOverCommandExecutor(nnCommands)).start();
                lastFailOver = System.currentTimeMillis();
                log.add("#NameNode Restart Initiated");
                Logger.printMsg("#NameNode Restart Initiated");
              }
            }
          }
        }

        try {
          Thread.sleep(1000); //[s] this is not very precise. TODO subtract the time spent in the the while loop
          tick++;
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    }

    public void stop() {
      stop = true;
    }

    public List<String> getFailoverLog() {
      return log;
    }
  }

  class FailOverCommandExecutor implements Runnable {
    List<String> commands;

    FailOverCommandExecutor(List<String> commands) {
      this.commands = commands;
    }

    @Override
    public void run() {
      for (String command : commands) {
        runCommand(command);
      }
    }

  }

  private void runCommand(String command) {
    try {
      Logger.printMsg("Going to execute command " + command);
      Process p = Runtime.getRuntime().exec(command);
      printErrors(p.getErrorStream());
      printErrors(p.getInputStream());
      p.waitFor();

      if (command.contains("kill")) { //[s] for some reason NameNode does not start soon after it is killed. TODO: fix it
        Thread.sleep(1000);
      }
    } catch (IOException e) {
      e.printStackTrace();
      Logger.printMsg("Exception During Restarting the NameNode Command " + command + "   Ex: " + e.toString());
    } catch (InterruptedException e) {
      Logger.error(e);
      Logger.printMsg("Exception During Restarting the NameNode Command " + command + "   Ex: " + e.toString());
    }
  }

  private void printErrors(InputStream errorStream) throws IOException {
    String line;
    BufferedReader input = new BufferedReader(new InputStreamReader(errorStream));
    while ((line = input.readLine()) != null) {
      Logger.printMsg(line);
    }
    input.close();
  }

}
