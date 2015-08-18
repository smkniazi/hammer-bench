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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.hops.experiments.benchmarks.common.NamespaceWarmUp;
import io.hops.experiments.controller.Logger;
import io.hops.experiments.controller.commands.WarmUpCommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import io.hops.experiments.benchmarks.common.Benchmark;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.utils.BenchmarkUtils;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.workload.generator.FilePool;

/**
 *
 * @author salman
 */
public class RawBenchmark extends Benchmark {

    private AtomicInteger successfulOps = new AtomicInteger(0);
    private AtomicInteger failedOps = new AtomicInteger(0);
    private long phaseStartTime;
    private long phaseDurationInMS;
    private int maxFilesToCreate = Integer.MAX_VALUE;
    private long deletePhaseFinishTime;
    private String baseDir;
    private short replicationFactor;
    private long fileSize;

    //-- other
    private ExecutorService executor;

    public RawBenchmark(Configuration conf, int numThreads) {
        super(conf, numThreads);
        executor = Executors.newFixedThreadPool(numThreads);
    }

    @Override
    protected WarmUpCommand.Response warmUp(WarmUpCommand.Request warmUpCommand)
        throws IOException, InterruptedException {
        NamespaceWarmUp.Request namespaceWarmUp = (NamespaceWarmUp.Request) warmUpCommand;
        this.replicationFactor = namespaceWarmUp.getReplicationFactor();
        this.fileSize = namespaceWarmUp.getFileSize();
        this.baseDir = namespaceWarmUp.getBaseDir();
        List workers = new ArrayList<WarmUp>();
        for (int i = 0; i < numThreads; i++) {
            Callable worker = new WarmUp(namespaceWarmUp.getFilesToCreate(), replicationFactor,
                fileSize, baseDir);
            workers.add(worker);
        }
        executor.invokeAll(workers); // blocking call
        return new NamespaceWarmUp.Response();
    }

    public class WarmUp implements Callable {

        private DistributedFileSystem dfs;
        private FilePool filePool;
        private int filesToCreate;
        private short replicationFactor;
        private long fileSize;
        private String baseDir;

        public WarmUp(int filesToCreate, short replicationFactor, long fileSize, String baseDir) throws IOException {
            this.filesToCreate = filesToCreate;
            this.fileSize = fileSize;
            this.replicationFactor = replicationFactor;
            this.baseDir = baseDir;
        }

        @Override
        public Object call() throws Exception {
            dfs = BenchmarkUtils.getDFSClient(conf);
            filePool = BenchmarkUtils.getFilePool(conf, baseDir);
            String filePath = null;

            for (int i = 0; i < filesToCreate; i++) {
                try {
                    filePath = filePool.getFileToCreate();
                    BenchmarkUtils
                        .createFile(dfs, new Path(filePath), replicationFactor,
                            fileSize);
                } catch (Exception e) {
                    if (filePath != null) {
                        filePool.fileCreationFailed(filePath);
                    }
                    Logger.error(e);

                }
            }
            return null;
        }
    };

    @Override
    protected BenchmarkCommand.Response processCommandInternal(BenchmarkCommand
        .Request command)
        throws IOException, InterruptedException {
        RawBenchmarkCommand.Request request = (RawBenchmarkCommand.Request) command;
        RawBenchmarkCommand.Response response;
        System.out.println("Starting the " + request.getPhase()+" duration "+request.getDurationInMS());
        if (request.getPhase() == BenchmarkOperations.MKDIRS) {
            response = startMkdirsPhase(request.getDurationInMS(), baseDir);
        }else if (request.getPhase() == BenchmarkOperations.CREATE_FILE) {
            RawBenchmarkCreateCommand.Request createCommand = (RawBenchmarkCreateCommand.Request) request;
            response = startWritePhase(createCommand.getDurationInMS(),
                replicationFactor,
                fileSize, baseDir);
        } else if (request.getPhase() == BenchmarkOperations.READ_FILE) {
            response = startReadPhase(request.getDurationInMS(), fileSize, baseDir);
        } else if (request.getPhase() == BenchmarkOperations.RENAME_FILE) {
            response = startRenamePhase(request.getDurationInMS(), baseDir);
        } else if (request.getPhase() == BenchmarkOperations.DELETE_FILE) {
            response = startDeletePhase(request.getDurationInMS(), baseDir);
        } else if (request.getPhase() == BenchmarkOperations.STAT_DIR || request.getPhase() == BenchmarkOperations.STAT_FILE) {
            response =
                startStatPhase(request.getDurationInMS(), baseDir, request
                    .getPhase());
        } else if (request.getPhase() == BenchmarkOperations.CHMOD_FILE || 
                request.getPhase() == BenchmarkOperations.CHMOD_DIR) {
            response = startChmodPhase(request.getPhase(), request.getDurationInMS(), baseDir);
        } else {
            throw new IllegalStateException();
        }
        return response;
    }

    private RawBenchmarkCommand.Response startMkdirsPhase(long duration, String
        baseDir) throws InterruptedException, UnknownHostException, IOException {
        List workers = new LinkedList<Mkdirs>();
        for (int i = 0; i < numThreads; i++) {
            Callable worker = new Mkdirs(baseDir);
            workers.add(worker);
        }
        setMeasurementVariables(duration);
        executor.invokeAll(workers);// blocking call

        double speed = ((double)successfulOps.get() / (double) phaseDurationInMS); // p / ms
        speed = speed * 1000;

        RawBenchmarkCommand.Response response =
                new RawBenchmarkCommand.Response(BenchmarkOperations.MKDIRS,
                phaseDurationInMS, successfulOps.get(), failedOps.get(), speed);
        return response;
    }

    public class Mkdirs implements Callable {

        private DistributedFileSystem dfs;
        private FilePool filePool;
        private String baseDir;

        public Mkdirs(String baseDir) throws IOException {
            this.baseDir = baseDir;
        }

        @Override
        public Object call() throws Exception {
            dfs = BenchmarkUtils.getDFSClient(conf);
            filePool = BenchmarkUtils.getFilePool(conf, baseDir);
            String filePath;

            while (true) {
                try {
                    if ((System.currentTimeMillis() - phaseStartTime) > (phaseDurationInMS)) {
                        return null;
                    }
                    filePath = filePool.getDirToCreate();

                    BenchmarkUtils.mkdirs(dfs, new Path(filePath));
                    successfulOps.incrementAndGet();
                    if (Logger.canILog()) {
                        Logger.printMsg("Successful mkdirs ops " + successfulOps.get() + " Failed read ops " + failedOps.get() + " Speed " + speedPSec(successfulOps, phaseStartTime) + " ops/sec");
                    }
                } catch (Exception e) {
                    failedOps.incrementAndGet();
                    Logger.error(e);

                }
            }
        }
    }
    
    
    private RawBenchmarkCommand.Response startWritePhase(long duration,
            short replicationFactor, long fileSize, String baseDir) throws InterruptedException, UnknownHostException, IOException {
        List workers = new ArrayList<Writer>();
        for (int i = 0; i < numThreads; i++) {
            Callable worker = new Writer(replicationFactor, baseDir, fileSize);
            workers.add(worker);
        }
        setMeasurementVariables(duration);
        executor.invokeAll(workers); // blocking call

        double speed = ((double)successfulOps.get() / (double) phaseDurationInMS); // p / ms
        speed = speed * 1000;

        RawBenchmarkCommand.Response response =
                new RawBenchmarkCommand.Response(BenchmarkOperations.CREATE_FILE,
                phaseDurationInMS, successfulOps.get(), failedOps.get(), speed);
        return response;
    }

    public class Writer implements Callable {

        private DistributedFileSystem dfs;
        private FilePool filePool;
        private String baseDir;
        private short replicationFactor;
        private long fileSize;

        public Writer(short replicationFactor, String baseDir, long fileSize) throws IOException {
            this.baseDir = baseDir;
            this.replicationFactor = replicationFactor;
            this.fileSize = fileSize;
        }

        @Override
        public Object call() throws Exception {
            dfs = BenchmarkUtils.getDFSClient(conf);
            filePool = BenchmarkUtils.getFilePool(conf, baseDir);
            String filePath = null;
            while (true) {
                try {
                    if (((System.currentTimeMillis() - phaseStartTime) > (phaseDurationInMS)) || (successfulOps.get() >= maxFilesToCreate)) {
                        return null;
                    }
                    filePath = filePool.getFileToCreate();
                    BenchmarkUtils
                        .createFile(dfs, new Path(filePath), replicationFactor,
                            fileSize);
                    successfulOps.incrementAndGet();
                    if (Logger.canILog()) {
                        Logger.printMsg("Successful write ops " + successfulOps.get() + " Failed write ops " + failedOps.get() + " Write Speed " + speedPSec(successfulOps, phaseStartTime) + " ops/sec ");
                    }
                } catch (Exception e) {
                    failedOps.incrementAndGet();
                    filePool.fileCreationFailed(filePath);
                    Logger.error(e);

                }
            }
        }
    }

    private RawBenchmarkCommand.Response startReadPhase(long duration, long
        fileSize, String baseDir) throws InterruptedException, UnknownHostException, IOException {
        List workers = new LinkedList<Reader>();
        for (int i = 0; i < numThreads; i++) {
            Callable worker = new Reader(baseDir, fileSize);
            workers.add(worker);
        }
        setMeasurementVariables(duration);
        executor.invokeAll(workers);// blocking call

        double speed = ((double)successfulOps.get() / (double) phaseDurationInMS); // p / ms
        speed = speed * 1000;

        RawBenchmarkCommand.Response response =
                new RawBenchmarkCommand.Response(BenchmarkOperations.READ_FILE,
                phaseDurationInMS, successfulOps.get(), failedOps.get(), speed);
        return response;
    }

    public class Reader implements Callable {

        private DistributedFileSystem dfs;
        private FilePool filePool;
        private String baseDir;
        private long fileSize;

        public Reader(String baseDir, long fileSize) throws IOException {
            this.baseDir = baseDir;
            this.fileSize = fileSize;
        }

        @Override
        public Object call() throws Exception {
            dfs = BenchmarkUtils.getDFSClient(conf);
            filePool = BenchmarkUtils.getFilePool(conf, baseDir);
            String filePath;

            while (true) {
                try {
                    if ((System.currentTimeMillis() - phaseStartTime) > (phaseDurationInMS)) {
                        return null;
                    }
                    filePath = filePool.getFileToRead();
                    BenchmarkUtils.readFile(dfs, new Path(filePath), fileSize);
                    successfulOps.incrementAndGet();
                    if (Logger.canILog()) {
                        Logger.printMsg("Successful read ops " + successfulOps.get() + " Failed read ops " + failedOps.get() + " Speed " + speedPSec(successfulOps, phaseStartTime) + " ops/sec");
                    }
                } catch (Exception e) {
                    failedOps.incrementAndGet();
                    Logger.error(e);

                }
            }
        }
    }

    private RawBenchmarkCommand.Response startRenamePhase(long duration, String
        baseDir) throws InterruptedException, UnknownHostException, IOException {
        List workers = new LinkedList<Renamer>();
        for (int i = 0; i < numThreads; i++) {
            Callable worker = new Renamer(baseDir);
            workers.add(worker);
        }
        setMeasurementVariables(duration);
        executor.invokeAll(workers);// blocking call

        double speed = ((double)successfulOps.get() / (double) phaseDurationInMS); // p / ms
        speed = speed * 1000;

        RawBenchmarkCommand.Response response =
                new RawBenchmarkCommand.Response(BenchmarkOperations.RENAME_FILE,
                phaseDurationInMS, successfulOps.get(), failedOps.get(), speed);
        return response;
    }

    public class Renamer implements Callable {

        private DistributedFileSystem dfs;
        private FilePool filePool;
        private String baseDir;
        public Renamer(String baseDir) throws IOException {
            this.baseDir = baseDir;
        }

        @Override
        public Object call() throws Exception {
            dfs = BenchmarkUtils.getDFSClient(conf);
            filePool = BenchmarkUtils.getFilePool(conf, baseDir);
            String from;
            String to;

            while (true) {
                try {
                    if ((System.currentTimeMillis() - phaseStartTime) > (phaseDurationInMS)) {
                        return null;
                    }
                    from = filePool.getFileToRename();
                    to = from + "_rnd";
                    if (BenchmarkUtils
                        .renameFile(dfs, new Path(from), new Path(to))) {
                        successfulOps.incrementAndGet();
                        filePool.fileRenamed(from, to);
                    } else {
                        failedOps.incrementAndGet();
                    }
                    if (Logger.canILog()) {
                        Logger.printMsg("Successful rename ops " + successfulOps.get() + " Failed rename ops " + failedOps.get() + " Speed: " + speedPSec(successfulOps, phaseStartTime));
                    }
                } catch (Exception e) {
                    failedOps.incrementAndGet();
                    Logger.error(e);

                }
            }
        }
    }

    private RawBenchmarkCommand.Response startDeletePhase(long duration, String
        baseDir) throws InterruptedException, UnknownHostException, IOException {
        List workers = new LinkedList<Eraser>();
        for (int i = 0; i < numThreads; i++) {
            Callable worker = new Eraser(baseDir);
            workers.add(worker);
        }
        setMeasurementVariables(duration);
        executor.invokeAll(workers);// blocking call
        deletePhaseFinishTime = System.currentTimeMillis();

        double speed = ((successfulOps.get()) / (double) ((deletePhaseFinishTime - phaseStartTime))); // p / ms
        speed = speed * 1000;

        RawBenchmarkCommand.Response response =
                new RawBenchmarkCommand.Response(BenchmarkOperations.DELETE_FILE,
                (deletePhaseFinishTime - phaseStartTime), successfulOps.get(), failedOps.get(), speed);
        return response;

    }

    public class Eraser implements Callable {

        private DistributedFileSystem dfs;
        private FilePool filePool;
        private String baseDir;

        public Eraser(String baseDir) throws IOException {
            this.baseDir = baseDir;
        }

        @Override
        public Object call() throws Exception {
            dfs = BenchmarkUtils.getDFSClient(conf);
            filePool = BenchmarkUtils.getFilePool(conf, baseDir);
            String file;
            while (true) {
                try {
                    file = filePool.getFileToDelete();
                    if (file == null || ((System.currentTimeMillis() - phaseStartTime) > (phaseDurationInMS))) {
                        return null;
                    }
                    if (BenchmarkUtils.deleteFile(dfs, new Path(file))) {
                        successfulOps.incrementAndGet();
                    } else {
                        failedOps.incrementAndGet();
                    }
                    if (Logger.canILog()) {
                        Logger.printMsg("Successful delete ops " + successfulOps.get() + " Failed delete ops " + failedOps.get() + " Speed: " + speedPSec(successfulOps, phaseStartTime));
                    }

                } catch (Exception e) {
                    failedOps.incrementAndGet();
                    Logger.error(e);
                }
            }
        }
    }

    private RawBenchmarkCommand.Response startStatPhase(long duration, String
        baseDir, BenchmarkOperations opType) throws InterruptedException, UnknownHostException, IOException {
        List workers = new LinkedList<StatPath>();
        for (int i = 0; i < numThreads; i++) {
            Callable worker = new StatPath(baseDir, opType);
            workers.add(worker);
        }
        setMeasurementVariables(duration);
        executor.invokeAll(workers);// blocking call

        double speed = ((double)successfulOps.get() / (double) phaseDurationInMS); // p / ms
        speed = speed * 1000;

        RawBenchmarkCommand.Response response =
                new RawBenchmarkCommand.Response(opType,
                phaseDurationInMS, successfulOps.get(), failedOps.get(), speed);
        return response;
    }

    public class StatPath implements Callable {

        private DistributedFileSystem dfs;
        private FilePool filePool;
        private String baseDir;
        private BenchmarkOperations opType;
        public StatPath(String baseDir, BenchmarkOperations opType) throws IOException {
            this.baseDir = baseDir;
            this.opType = opType;
        }

        @Override
        public Object call() throws Exception {
            dfs = BenchmarkUtils.getDFSClient(conf);
            filePool = BenchmarkUtils.getFilePool(conf, baseDir);
            String path;
            while (true) {
                try {
                    if(opType == BenchmarkOperations.STAT_FILE){
                        path = filePool.getFileToStat();
                    }else if (opType == BenchmarkOperations.STAT_DIR){
                        path = filePool.getDirToStat();
                    }else{
                        throw new UnsupportedOperationException("Unsupported Stat operation");
                    }
                    
                    if (path == null || ((System.currentTimeMillis() - phaseStartTime) > (phaseDurationInMS))) {
                        return null;
                    }
                    BenchmarkUtils.stat(dfs, new Path(path));
                    successfulOps.incrementAndGet();

                    if (Logger.canILog()) {
                        Logger.printMsg("Successful "+opType+" ops " + successfulOps.get() + " Failed ops " + failedOps.get() + " Speed: " + speedPSec(successfulOps, phaseStartTime));
                    }
                } catch (Exception e) {
                    failedOps.incrementAndGet();
                    Logger.error(e);
                }
            }
        }
    }

    
    private RawBenchmarkCommand.Response startChmodPhase(BenchmarkOperations opType,long duration, String
        baseDir) throws InterruptedException, UnknownHostException, IOException {
        List workers = new LinkedList<Chmod>();
        for (int i = 0; i < numThreads; i++) {
            Callable worker = new Chmod(baseDir, opType == BenchmarkOperations.CHMOD_DIR);
            workers.add(worker);
        }
        setMeasurementVariables(duration);
        executor.invokeAll(workers);// blocking call

        double speed = ((double)successfulOps.get() / (double) phaseDurationInMS); // p / ms
        speed = speed * 1000;

        RawBenchmarkCommand.Response response =
                new RawBenchmarkCommand.Response(opType,
                phaseDurationInMS, successfulOps.get(), failedOps.get(), speed);
        return response;
    }

    public class Chmod implements Callable {

        private DistributedFileSystem dfs;
        private FilePool filePool;
        private String baseDir;
        private boolean isDirOp;

        public Chmod(String baseDir,boolean isDirOp) throws IOException {
            this.baseDir = baseDir;
            this.isDirOp = isDirOp;
        }

        @Override
        public Object call() throws Exception {
            dfs = BenchmarkUtils.getDFSClient(conf);
            filePool = BenchmarkUtils.getFilePool(conf, baseDir);
            String path;
            while (true) {
                try {
                    if(isDirOp){
                      path = filePool.getDirPathToChangePermissions();
                    }else{
                      path = filePool.getFilePathToChangePermissions();
                    }
                                        
                    if (path == null || ((System.currentTimeMillis() - phaseStartTime) > (phaseDurationInMS))) {
                        return null;
                    }

                    BenchmarkUtils.chmodPath(dfs, new Path(path));
                    
                    successfulOps.incrementAndGet();

                    if (Logger.canILog()) {
                        Logger.printMsg("Successful Chmod ("+(isDirOp?"Dir":"File")+") ops " + successfulOps.get() + " Failed chmod ops " + failedOps.get() + " Speed: " + speedPSec(successfulOps, phaseStartTime));
                    }
                } catch (Exception e) {
                    failedOps.incrementAndGet();
                    Logger.error(e);
                }
            }
        }
    }
    
    private void setMeasurementVariables(long duration){
        phaseDurationInMS = duration;
        phaseStartTime = System.currentTimeMillis();
        successfulOps = new AtomicInteger(0);
        failedOps = new AtomicInteger(0);
    }
    
    public double speedPSec(AtomicInteger ops, long startTime) {
        long timePassed = (System.currentTimeMillis() - startTime);
        double opsPerMSec = (double) (ops.get()) / (double) timePassed;
        return opsPerMSec * 1000;
    }

}
