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

import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.benchmarks.common.BenchmarkType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author salman
 */
public class InterleavedBenchmarkCommand {

    public static class Request implements BenchmarkCommand.Request {

        private BigDecimal createPercent;
        private BigDecimal appendPercent;
        private BigDecimal readPercent;
        private BigDecimal renamePercent;
        private BigDecimal deletePercent;
        private BigDecimal lsFilePercent;
        private BigDecimal lsDirPercent;
        private BigDecimal chmodFilePercent;
        private BigDecimal chmodDirsPercent;
        private BigDecimal mkdirPercent;
        private BigDecimal setReplicationPercent;
        private BigDecimal fileInfoPercent;
        private BigDecimal dirInfoPercent;
        private BigDecimal fileChownPercent;
        private BigDecimal dirChownPercent;
        private long duration;
        private long fileSize;
        private long appendSize;
        private short replicationFactor;
        private String baseDir;
        private boolean percentileEnabled;
        private boolean testFailover;
        private List<List<String>> namenodeRestartCommands;
        private long namenodeRestartTimePeriod;
        private long failTestDuration;
        private long failOverTestStartTime;


        public Request(BigDecimal createPercent, BigDecimal appendPercent, BigDecimal readPercent, BigDecimal renamePercent, BigDecimal deletePercent, BigDecimal lsFilePercent, BigDecimal lsDirPercent,
                       BigDecimal chmodFilesPercent, BigDecimal chmodDirsPercent, BigDecimal mkdirPercent,
                       BigDecimal setReplicationPercent, BigDecimal fileInfoPercent, BigDecimal dirInfoPercent,
                       BigDecimal fileChownPercent, BigDecimal dirChownPercent,
                       long duration, long fileSize, long appendSize, short replicationFactor, String baseDir,
                       boolean percentileEnabled, boolean testFailover, List<List<String>> namenodeRestartCommands,
                       long namenodeRestartTimePeriod, long failTestDuration, long failOverTestStartTime) {
            this.createPercent = createPercent;
            this.appendPercent = appendPercent;
            this.readPercent = readPercent;
            this.renamePercent = renamePercent;
            this.deletePercent = deletePercent;
            this.lsFilePercent = lsFilePercent;
            this.lsDirPercent = lsDirPercent;
            this.chmodFilePercent = chmodFilesPercent;
            this.chmodDirsPercent = chmodDirsPercent;
            this.mkdirPercent = mkdirPercent;
            this.setReplicationPercent = setReplicationPercent;
            this.fileInfoPercent = fileInfoPercent;
            this.dirInfoPercent = dirInfoPercent;
            this.fileChownPercent = fileChownPercent;
            this.dirChownPercent = dirChownPercent;
            this.duration = duration;
            this.fileSize = fileSize;
            this.appendSize = appendSize;
            this.replicationFactor = replicationFactor;
            this.baseDir = baseDir;
            this.percentileEnabled = percentileEnabled;
            this.testFailover = testFailover;
            this.namenodeRestartCommands = namenodeRestartCommands;
            this.namenodeRestartTimePeriod = namenodeRestartTimePeriod;
            this.failOverTestStartTime = failOverTestStartTime;
            this.failTestDuration = failTestDuration;
        }

        public long getFailTestDuration() {
            return failTestDuration;
        }

        public long getFailOverTestStartTime() {
            return failOverTestStartTime;
        }

        public boolean isPercentileEnabled() {
            return percentileEnabled;
        }

        public BigDecimal getCreatePercent() {
            return createPercent;
        }

        public BigDecimal getReadPercent() {
            return readPercent;
        }

        public BigDecimal getRenamePercent() {
            return renamePercent;
        }

        public BigDecimal getDeletePercent() {
            return deletePercent;
        }

        public BigDecimal getLsFilePercent() {
            return lsFilePercent;
        }

        public BigDecimal getLsDirPercent() {
            return lsDirPercent;
        }

        public BigDecimal getChmodFilePercent() {
            return chmodFilePercent;
        }

        public BigDecimal getChmodDirsPercent() {
            return chmodDirsPercent;
        }

        public long getDuration() {
            return duration;
        }

        public long getFileSize() {
            return fileSize;
        }

        public short getReplicationFactor() {
            return replicationFactor;
        }

        public String getBaseDir() {
            return baseDir;
        }

        public BigDecimal getMkdirPercent() {
            return mkdirPercent;
        }

        public BigDecimal getSetReplicationPercent() {
            return setReplicationPercent;
        }

        public BigDecimal getFileInfoPercent() {
            return fileInfoPercent;
        }

        public BigDecimal getDirInfoPercent() {
            return dirInfoPercent;
        }

        public BigDecimal getAppendPercent() {
            return appendPercent;
        }

        public long getAppendSize() {
            return appendSize;
        }

        public BigDecimal getFileChownPercent() {
            return fileChownPercent;
        }

        public BigDecimal getDirChownPercent() {
            return dirChownPercent;
        }

        public boolean isTestFailover() {
            return testFailover;
        }

        public List<List<String>> getNamenodeRestartCommands() {
            return namenodeRestartCommands;
        }

        public long getNamenodeRestartTimePeriod() {
            return namenodeRestartTimePeriod;
        }

        @Override
        public BenchmarkType getBenchMarkType() {
            return BenchmarkType.INTERLEAVED;
        }
    }

    public static class Response implements BenchmarkCommand.Response {

        private final long runTime;
        private final long totalSuccessfulOps;
        private final long totalFailedOps;
        private final double opsPerSec;
        private final double avgOpLatency;
        private final HashMap<BenchmarkOperations, ArrayList<Long>> opsExeTimes;
        private final List<String> failOverLog;

        public Response(long runTime, long totalSuccessfulOps, long totalFailedOps, double opsPerSec,
                        HashMap<BenchmarkOperations, ArrayList<Long>> opsExeTimes, double avgOpLatency, List<String> failOverLog) {
            this.runTime = runTime;
            this.totalSuccessfulOps = totalSuccessfulOps;
            this.totalFailedOps = totalFailedOps;
            this.opsPerSec = opsPerSec;
            this.opsExeTimes = opsExeTimes;
            this.failOverLog = failOverLog;
            this.avgOpLatency = avgOpLatency;
        }

        public HashMap<BenchmarkOperations, ArrayList<Long>> getOpsExeTimes() {
            return opsExeTimes;
        }


        public long getRunTime() {
            return runTime;
        }

        public long getTotalSuccessfulOps() {
            return totalSuccessfulOps;
        }

        public long getTotalFailedOps() {
            return totalFailedOps;
        }

        public double getOpsPerSec() {
            return opsPerSec;
        }

        public List<String> getFailOverLog() {
            return failOverLog;
        }

        public double getAvgOpLatency() {
            return avgOpLatency;
        }
    }
}
