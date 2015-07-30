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

import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.common.BenchmarkType;


/**
 *
 * @author salman
 */
public class RawBenchmarkCommand {
     public static class Request implements BenchmarkCommand.Request{

        private final BenchmarkOperations phase;
        private final long duration;

        public Request(BenchmarkOperations phase, long duration) {
            this.phase = phase;
            this.duration = duration;
        }

        public BenchmarkOperations getPhase() {
            return phase;
        }

        public long getDurationInMS() {
            return duration;
        }

        @Override
        public BenchmarkType getBenchMarkType() {
            return BenchmarkType.RAW;
        }
    }

    public static class Response implements BenchmarkCommand.Response{

        private final BenchmarkOperations phase;
        private final long runTime;
        private final long totalSuccessfulOps;
        private final long totalFailedOps;
        private final double opsPerSec;

        public Response(BenchmarkOperations phase, long runTime, long totalSuccessfulOps, long totalFailedOps, double opsPerSec) {
            this.phase = phase;
            this.runTime = runTime;
            this.totalSuccessfulOps = totalSuccessfulOps;
            this.totalFailedOps = totalFailedOps;
            this.opsPerSec = opsPerSec;
        }

        public BenchmarkOperations getPhase() {
            return phase;
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
    }
}
