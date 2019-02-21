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
package io.hops.experiments.controller.commands;

import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
import io.hops.experiments.benchmarks.common.BenchmarkType;
import io.hops.experiments.benchmarks.common.config.BMConfiguration;

import java.io.Serializable;
import java.util.Properties;


/**
 *
 * @author salman
 */
public class Handshake implements Serializable {

  public static class Request implements Serializable {

    BMConfiguration bmConf;
    private  int slaveId;

    public Request(BMConfiguration bmConf) {
      this.bmConf = bmConf;
    }

    public BMConfiguration getBmConf() {
      return bmConf;
    }

    public int getSlaveId() {
      return slaveId;
    }

    public void setSlaveId(int slaveId) {
      this.slaveId = slaveId;
    }
  }

  public static class Response implements Serializable {
  }
}
