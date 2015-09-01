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
package io.hops.experiments.controller;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import io.hops.experiments.controller.commands.BenchmarkCommand;
import io.hops.experiments.controller.commands.Handshake;
import io.hops.experiments.controller.commands.KillSlave;
import org.apache.hadoop.conf.Configuration;
import io.hops.experiments.benchmarks.common.Benchmark;

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
    private DatagramSocket slaveSocket = null;
    private InetAddress masterIP = null;
    private int masterPort = 0;
    private Benchmark benchmark;
    private SlaveArgsReader args;
    private Handshake.Request handShake = null;
    private Configuration dfsClientConf;

    public void start(String configFilePath) throws Exception {
        args = new SlaveArgsReader(configFilePath);
        bind();
        handShakeWithMaster(); // Let all the clients know show is the master 
       //warmUp already handled by startlistener
        startListener();
    }

    private void handShakeWithMaster() throws IOException, ClassNotFoundException {
        System.out.println("Waiting for hand shake ... ");

        Object obj = receiveRequestFromMaster();

        if (obj instanceof Handshake.Request) {
            handShake = (Handshake.Request) obj;
            createHdfsConf(handShake);
            benchmark = Benchmark.getBenchmark(handShake.getBenchMarkType(),
                handShake.getNumThreads(), dfsClientConf, handShake.getSlaveId(),
                handShake.getInodesPerDir());
            if (handShake.isEnableRemoteLogging()) {
                Logger.setEnableRemoteLogging(true);
                Logger.setLoggerIp(masterIP);
                Logger.setLoggerPort(handShake.getRemoteLoggingPort());
            }

            sendResponseToMaster(new Handshake.Response());
        } else {
            throw new IllegalStateException("Hand shake phase. Got unknown request : " + obj);
        }
    }

    private void startListener() throws IOException, ClassNotFoundException, InterruptedException {
        while (true) {
            Object obj = receiveRequestFromMaster();
            if (obj instanceof BenchmarkCommand.Request) {
                BenchmarkCommand.Request command = (BenchmarkCommand.Request) obj;
                if (!command.getBenchMarkType().equals(handShake.getBenchMarkType())) {
                    throw new IllegalStateException("BenchMarkType Mismatch. Expecting " + handShake.getBenchMarkType() + " Got: " + command.getBenchMarkType());
                }

                sendResponseToMaster(benchmark.processCommand(command));
            }
        }
    }

    private void bind() throws SocketException, UnknownHostException {
        slaveSocket = new DatagramSocket(args.getSlaveListeningPort(), InetAddress.getByName("0.0.0.0"));
    }

    private Object receiveRequestFromMaster() throws IOException, ClassNotFoundException {
        byte[] recvData = new byte[ConfigKeys.BUFFER_SIZE];
        DatagramPacket recvPacket = new DatagramPacket(recvData, recvData.length);
        slaveSocket.receive(recvPacket);
        byte[] data = recvPacket.getData();
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);

        if (masterIP == null && masterPort == 0) {// save the address of master 
            masterIP = recvPacket.getAddress();
            masterPort = recvPacket.getPort();
        }

        Object obj = is.readObject();
        if (obj instanceof KillSlave) {
            System.exit(0);
        }

        return obj;
    }

    private void sendResponseToMaster(Object obj) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(outputStream);
        os.writeObject(obj);
        byte[] data = outputStream.toByteArray();

        DatagramPacket packet = new DatagramPacket(data, data.length,
                masterIP, masterPort);
        slaveSocket.send(packet);
    }
    
    private void createHdfsConf(Handshake.Request request){
        dfsClientConf = new Configuration();
        dfsClientConf.set(ConfigKeys.FS_DEFAULTFS_KEY, request.getNamenodeRpcAddress());
        dfsClientConf.set(ConfigKeys.DFS_CLIENT_REFRESH_NAMENODE_LIST_KEY,
            Long.toString(request.getNameNodeListRefreshTime()));
        dfsClientConf.set(ConfigKeys.DFS_NAMENODE_SELECTOR_POLICY_KEY,
            request.getNamenodeSelectionPolicy());
    }
}
