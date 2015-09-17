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
import io.hops.experiments.benchmarks.interleaved.InterleavedBenchmark;
import io.hops.experiments.benchmarks.rawthroughput.RawBenchmark;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.controller.commands.WarmUpCommand;
import io.hops.experiments.utils.BenchmarkUtils;
import io.hops.experiments.workload.generator.FilePool;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public abstract class Benchmark {

  protected final Configuration conf;
  protected final int numThreads;
  protected final ExecutorService executor;

  public Benchmark(Configuration conf, int numThreads) {
    this.conf = conf;
    this.numThreads = numThreads;
    this.executor = Executors.newFixedThreadPool(numThreads);
  }

  protected abstract WarmUpCommand.Response warmUp(WarmUpCommand.Request warmUp)
          throws IOException, InterruptedException;

  protected abstract BenchmarkCommand.Response processCommandInternal(BenchmarkCommand.Request command) throws IOException,
          InterruptedException;

  public final BenchmarkCommand.Response processCommand(BenchmarkCommand.Request command)
          throws IOException, InterruptedException {
    if (command instanceof WarmUpCommand.Request) {
      return warmUp((WarmUpCommand.Request) command);
    }
    return processCommandInternal(command);
  }

  public static Benchmark getBenchmark(BenchmarkType type, int numThreads,
          Configuration conf, int slaveId, int dirsPerDir, int filesPerDir, long maxFilesToCreate) {
    if (type == BenchmarkType.RAW) {
      return new RawBenchmark(conf, numThreads, dirsPerDir, filesPerDir, maxFilesToCreate);
    } else if (type == BenchmarkType.INTERLEAVED) {
      return new InterleavedBenchmark(conf, numThreads, dirsPerDir, filesPerDir);
    } else if (type == BenchmarkType.BR) {
      return new BlockReportingBenchmark(conf, numThreads, slaveId);
    } else {
      throw new UnsupportedOperationException("Unsupported Benchmark " + type);
    }
  }
  
  public static AtomicLong filesCreatedInWarmupPhase = new AtomicLong(0);
  protected class BaseWarmUp implements Callable {

    private DistributedFileSystem dfs;
    private FilePool filePool;
    private int filesToCreate;
    private short replicationFactor;
    private long fileSize;
    private String baseDir;
    private int dirsPerDir;
    private int filesPerDir;

    public BaseWarmUp(int filesToCreate, short replicationFactor, long fileSize, String baseDir, int dirsPerDir, int filesPerDir) throws IOException {
      this.filesToCreate = filesToCreate;
      this.fileSize = fileSize;
      this.replicationFactor = replicationFactor;
      this.baseDir = baseDir;
      this.dirsPerDir = dirsPerDir;
      this.filesPerDir = filesPerDir;
    }

    @Override
    public Object call() throws Exception {
      dfs = BenchmarkUtils.getDFSClient(conf);
      filePool = BenchmarkUtils.getFilePool(conf, baseDir, dirsPerDir, filesPerDir);
      String filePath = null;

      for (int i = 0; i < filesToCreate; i++) {
        try {
          filePath = filePool.getFileToCreate();
          BenchmarkUtils
                  .createFile(dfs, new Path(filePath), replicationFactor,
                  fileSize);
          filePool.fileCreationSucceeded(filePath);
          BenchmarkUtils.readFile(dfs, new Path(filePath), fileSize);
          filesCreatedInWarmupPhase.incrementAndGet();
          log();
        } catch (Exception e) {
          Logger.error(e);
        }
      }
      log();
      return null;
    }

    private void log() {
      if (Logger.canILog()) {
        long totalFilesThatWillBeCreated = filesToCreate * numThreads;
        double percent = (filesCreatedInWarmupPhase.doubleValue() / totalFilesThatWillBeCreated) * 100;
        Logger.printMsg("Warmup " + BenchmarkUtils.round(percent) + "% completed");
      }
    }
  };
}
