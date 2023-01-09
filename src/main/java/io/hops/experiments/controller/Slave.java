/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.experiments.controller;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import io.hops.experiments.benchmarks.common.config.BMConfiguration;
import io.hops.experiments.benchmarks.common.config.SlaveArgsReader;
import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.controller.commands.Handshake;
import io.hops.experiments.controller.commands.KillSlave;
import io.hops.experiments.benchmarks.common.config.ConfigKeys;
import org.apache.hadoop.conf.Configuration;
import io.hops.experiments.benchmarks.common.Benchmark;

import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 * @author salman
 */
public class Slave {

  public static void main(String[] argv) throws Exception {
    System.out.println("*** Starting the humble slave ***");
    String configFilePath = "slave.properties";
    if (argv.length == 1) {
      configFilePath = argv[0];
    }
    new Slave().start(configFilePath);
  }

  private ServerSocket slaveServerSocket = null;
  private Socket connectionWithMaster = null;
  private InetAddress masterIP = null;
  private Benchmark benchmark;
  private SlaveArgsReader args;
  private Configuration dfsClientConf;
  private BMConfiguration bmConf;

  public void start(String configFilePath) throws Exception {
    args = new SlaveArgsReader(configFilePath);
    connect();
    handShakeWithMaster();
    startListener();
  }

  private void handShakeWithMaster() throws IOException, ClassNotFoundException {

    System.out.println("Waiting for handshake message ");
    Object obj = receiveRequestFromMaster();

    int slaveId = 0;
    if (obj instanceof Handshake.Request) {
      bmConf = ((Handshake.Request) obj).getBmConf();
      slaveId = ((Handshake.Request) obj).getSlaveId();
      if (bmConf.isEnableRemoteLogging()) {
        Logger.setEnableRemoteLogging(true);
        Logger.setLoggerIp(masterIP);
        Logger.setLoggerPort(bmConf.getRemoteLoggingPort());
      }
      dfsClientConf = new Configuration();
      for (Object key : bmConf.getFsConfig().keySet()) {
        String keyStr = (String) key;
        String val = bmConf.getFsConfig().getProperty(keyStr);
        //Logger.printMsg("Client Settings "+keyStr+" --> "+val);
        dfsClientConf.set(keyStr, val);
      }

      benchmark = Benchmark.getBenchmark(dfsClientConf, bmConf, slaveId);

      sendResponseToMaster(new Handshake.Response());
    } else {
      throw new IllegalStateException("Hand shake phase. Got unknown request : " + obj);
    }
  }

  private void startListener() throws Exception {
    while (true) {
      Object obj = receiveRequestFromMaster();
      if (obj instanceof BenchmarkCommand.Request) {
        BenchmarkCommand.Request command = (BenchmarkCommand.Request) obj;
        if (!command.getBenchMarkType().equals(bmConf.getBenchMarkType())) {
          throw new IllegalStateException("BenchMarkType Mismatch. Expecting " + bmConf.getBenchMarkType() + " Got: " + command.getBenchMarkType());
        }

        sendResponseToMaster(benchmark.processCommand(command));
      }
    }
  }

  private void connect() throws SocketException, UnknownHostException, IOException {
    System.out.println("Waiting for connection from master ... ");
    slaveServerSocket = new ServerSocket(args.getSlaveListeningPort());
    connectionWithMaster = slaveServerSocket.accept();
    masterIP = connectionWithMaster.getInetAddress();
    System.out.print("Connected to master");
  }

  private Object receiveRequestFromMaster() throws IOException, ClassNotFoundException {
    connectionWithMaster.setReceiveBufferSize(ConfigKeys.BUFFER_SIZE);
    ObjectInputStream recvFromMaster = new ObjectInputStream(connectionWithMaster.getInputStream());
    Object obj = recvFromMaster.readObject();
    if (obj instanceof KillSlave) {
      System.exit(0);
    }
    return obj;
  }

  private void sendResponseToMaster(Object obj) throws IOException {
    System.out.println("Sending response to master ... ");
    long startTime = System.currentTimeMillis();
    connectionWithMaster.setSendBufferSize(ConfigKeys.BUFFER_SIZE);
    ObjectOutputStream sendToMaster = new ObjectOutputStream(connectionWithMaster.getOutputStream());
    sendToMaster.writeObject(obj);
    System.out.println("Sent response to master. Time: " + (System.currentTimeMillis() - startTime) + " ms");
  }
}

