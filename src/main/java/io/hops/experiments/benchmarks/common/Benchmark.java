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
package io.hops.experiments.benchmarks.common;

import io.hops.experiments.benchmarks.blockreporting.BlockReportingBenchmark;
import io.hops.experiments.benchmarks.common.coin.FileSizeMultiFaceCoin;
import io.hops.experiments.benchmarks.interleaved.InterleavedBenchmark;
import io.hops.experiments.benchmarks.rawthroughput.RawBenchmark;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.controller.commands.Handshake;
import io.hops.experiments.controller.commands.WarmUpCommand;
import io.hops.experiments.utils.BenchmarkUtils;
import io.hops.experiments.workload.generator.FilePool;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class Benchmark {

  protected final Configuration conf;
  protected final int numThreads;
  protected final ExecutorService executor;
  private AtomicInteger threadsWarmedUp = new AtomicInteger(0);
  private final BenchMarkFileSystemName fsName;

  public Benchmark(Configuration conf, int numThreads, BenchMarkFileSystemName fsName) {
    this.conf = conf;
    this.numThreads = numThreads;
    this.executor = Executors.newFixedThreadPool(numThreads);
    this.fsName = fsName;
  }

  protected abstract WarmUpCommand.Response warmUp(WarmUpCommand.Request warmUp)
          throws Exception;

  protected abstract BenchmarkCommand.Response processCommandInternal(BenchmarkCommand.Request command) throws Exception,
          InterruptedException;

  public final BenchmarkCommand.Response processCommand(BenchmarkCommand.Request command)
          throws Exception {
    if (command instanceof WarmUpCommand.Request) {
      return warmUp((WarmUpCommand.Request) command);
    }
    return processCommandInternal(command);
  }
  
  public static Benchmark getBenchmark(Configuration conf, Handshake.Request handShake) {
    if (handShake.getBenchMarkType() == BenchmarkType.RAW) {
      return new RawBenchmark(conf, handShake.getNumThreads(), handShake.getDirPerDir(), handShake.getFilesPerDir(),
              handShake.getMaxFilesToCreate(), handShake.isFixedDepthTree(), handShake.getTreeDepth(),handShake.getBenchMarkFileSystemName());
    } else if (handShake.getBenchMarkType() == BenchmarkType.INTERLEAVED) {
      return new InterleavedBenchmark(conf, handShake.getNumThreads(), handShake.getDirPerDir(), handShake.getFilesPerDir(),
               handShake.isFixedDepthTree(), handShake.getTreeDepth(),handShake.getBenchMarkFileSystemName());
    } else if (handShake.getBenchMarkType() == BenchmarkType.BR) {
//        throw new UnsupportedOperationException("BR is commented out as it is only supported for hadoop 2.0.4-alpha");
         return new BlockReportingBenchmark(conf, handShake.getNumThreads(), handShake.getSlaveId(),
              handShake.getBenchMarkFileSystemName());
    } else {
      throw new UnsupportedOperationException("Unsupported Benchmark " + handShake.getBenchMarkType());
    }
  }
  
  protected AtomicLong filesCreatedInWarmupPhase = new AtomicLong(0);
  protected class BaseWarmUp implements Callable {

    private FileSystem dfs;
    private FilePool filePool;
    private final int filesToCreate;
    private final short replicationFactor;
    private final String fileSizeDistribution;
    private final FileSizeMultiFaceCoin fileSizeCoin;
    private final String baseDir;
    private final int dirsPerDir;
    private final int filesPerDir;
    private final boolean fixedDepthTree;
    private final int treeDepth;

    public BaseWarmUp(int filesToCreate, short replicationFactor, String fileSizeDistribution,
            String baseDir, int dirsPerDir, int filesPerDir,
            boolean fixedDepthTree, int treeDepth) throws IOException {
      this.filesToCreate = filesToCreate;
      this.fileSizeDistribution = fileSizeDistribution;
      this.fileSizeCoin = new FileSizeMultiFaceCoin(fileSizeDistribution);
      this.replicationFactor = replicationFactor;
      this.baseDir = baseDir;
      this.dirsPerDir = dirsPerDir;
      this.filesPerDir = filesPerDir;
      this.fixedDepthTree = fixedDepthTree;
      this.treeDepth = treeDepth;
    }

    @Override
    public Object call() throws Exception {
      dfs = BenchmarkUtils.getDFSClient(conf);
      filePool = BenchmarkUtils.getFilePool(conf, baseDir, dirsPerDir, 
              filesPerDir, fixedDepthTree, treeDepth );
      String filePath = null;

      for (int i = 0; i < filesToCreate; i++) {
        try {
          filePath = filePool.getFileToCreate();
          BenchmarkUtils
                  .createFile(dfs, new Path(filePath), replicationFactor,
                  fileSizeCoin.getFileSize());
          filePool.fileCreationSucceeded(filePath);
          BenchmarkUtils.readFile(dfs, new Path(filePath), fileSizeCoin.getFileSize());
          filesCreatedInWarmupPhase.incrementAndGet();
          log();
        } catch (Exception e) {
          Logger.error(e);
        }
      }
      log();
      threadsWarmedUp.incrementAndGet();
      while(threadsWarmedUp.get() != numThreads){ // this is to ensure that all the threads in the executor service are started during the warmup phase
        Thread.sleep(100);
      }

      System.out.println("WarmedUp");
      return null;
    }

    private void log() {
      if (Logger.canILog()) {
        long totalFilesThatWillBeCreated = filesToCreate * numThreads;
        double percent = (filesCreatedInWarmupPhase.doubleValue() / totalFilesThatWillBeCreated) * 100;
        Logger.printMsg("Warmup Phase: " + BenchmarkUtils.round(percent) + "%");
      }
    }
  };

  protected int getAliveNNsCount() throws IOException {
    FileSystem fs = BenchmarkUtils.getDFSClient(conf);
    int actualNNCount = 0;
    try {
      actualNNCount = BenchmarkUtils.getActiveNameNodesCount(fsName, fs);
    } catch (Exception e) {
      Logger.error(e);
    }
    return actualNNCount;
  }
}
