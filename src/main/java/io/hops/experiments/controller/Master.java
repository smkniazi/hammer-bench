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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.hops.experiments.benchmarks.blockreporting.BlockReportingBenchmark;
import io.hops.experiments.benchmarks.blockreporting.BlockReportingBenchmarkCommand;
import io.hops.experiments.benchmarks.blockreporting.BlockReportingWarmUp;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.common.BenchmarkType;
import io.hops.experiments.controller.commands.Handshake;
import io.hops.experiments.benchmarks.interleaved.InterleavedBenchmarkCommand;
import io.hops.experiments.controller.commands.KillSlave;
import io.hops.experiments.benchmarks.rawthroughput.RawBenchmarkCommand;
import io.hops.experiments.benchmarks.rawthroughput.RawBenchmarkCreateCommand;
import io.hops.experiments.benchmarks.common.NamespaceWarmUp;
import io.hops.experiments.controller.commands.WarmUpCommand;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author salman
 */
public class Master {

    public static void main(String[] argv) throws Exception {
        String configFilePath = "master.properties";
        if (argv.length == 1) {
            if (argv[0].compareToIgnoreCase("help") == 0) {
                MasterArgsReader.printHelp();
                System.exit(0);
            } else {
                configFilePath = argv[0];
            }
        }
        new Master().start(configFilePath);
    }
    private DatagramSocket masterSocket = null;
    MasterArgsReader args;

    public void start(String configFilePath) throws Exception {
        System.out.println("*** Starting the master ***");
        args = new MasterArgsReader(configFilePath);

        resetResultFile();

        startRemoteLogger();
        startListener();
        handShakeWithSlaves(); // Let all the clients know show is the master

        //send init to slaves
        warmUpSlaves();

        //start the commander
        startCommander();

        sendToAllSlaves(new KillSlave());
        
        printAllResults();

        System.exit(0);
    }

    private void startRemoteLogger() {
        Logger.LogListener listener = new Logger.LogListener(args.getRemoteLogginPort());
        Thread thread = new Thread(listener);
        thread.start();
    }

    private void startCommander() throws IOException, InterruptedException, ClassNotFoundException {
        if (args.getBenchMarkType() == BenchmarkType.RAW) {
            startRawCommander();
        } else if (args.getBenchMarkType() == BenchmarkType.INTERLEAVED) {
            startInterleavedCommander();
        }else if(args.getBenchMarkType() == BenchmarkType.BR){
            startBlockReportingCommander();
        }
    }

    private void startRawCommander() throws IOException, InterruptedException, ClassNotFoundException {
        
        if (args.getRawMkdirPhaseDuration()> 0) {
            startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
                BenchmarkOperations.MKDIRS, args.getRawMkdirPhaseDuration()));
        }
        
        if (args.getRawCreateFilesPhaseDuration() > 0) {
            startRawBenchmarkPhase(new RawBenchmarkCreateCommand.Request(
                args.getRawCreatePhaseMaxFilesToCreate(),
                BenchmarkOperations.CREATE_FILE,
                args.getRawCreateFilesPhaseDuration()));
        }

        if (args.getRawReadFilesPhaseDuration() > 0) {
            startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
                BenchmarkOperations.READ_FILE,
                args.getRawReadFilesPhaseDuration()));
        }

        if (args.getRawStatFilePhaseDuration() > 0) {
            startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
                BenchmarkOperations.STAT_FILE,
                args.getRawStatFilePhaseDuration()));
        }
        
        if (args.getRawStatDirPhaseDuration() > 0) {
            startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
                BenchmarkOperations.STAT_DIR,
                args.getRawStatDirPhaseDuration()));
        }

        if (args.getRawChmodFilesPhaseDuration() > 0) {
            startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
                BenchmarkOperations.CHMOD_FILE,
                args.getRawChmodFilesPhaseDuration()));
        }

        if (args.getRawRenameFilesPhaseDuration() > 0) {
            startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
                BenchmarkOperations.RENAME_FILE,
                args.getRawRenameFilesPhaseDuration()));
        }

        if (args.getRawDeleteFilesPhaseDuration() > 0) {
            startRawBenchmarkPhase(new RawBenchmarkCommand.Request(
                BenchmarkOperations.DELETE_FILE,
                args.getRawDeleteFilesPhaseDuration()));
        }
    }

    private void startBlockReportingCommander() throws IOException, ClassNotFoundException {
        System.out.println("Starting BlockReporting Benchmark ...");
        prompt();
        BlockReportingBenchmarkCommand.Request request = new
            BlockReportingBenchmarkCommand.Request(args
            .getBLockReportingNumOfReports(), args
            .getBlockReportingMinTimeBeforeNextReport(), args
            .getBlockReportingMaxTimeBeforeNextReport());

        sendToAllSlaves(request);

        Collection<Object> responses = receiveFromAllSlaves(Integer.MAX_VALUE);
        DescriptiveStatistics successfulOps = new DescriptiveStatistics();
        DescriptiveStatistics failedOps = new DescriptiveStatistics();
        DescriptiveStatistics speed = new DescriptiveStatistics();
        DescriptiveStatistics avgTimePerReport = new DescriptiveStatistics();
        DescriptiveStatistics avgTimeTogetANewNameNode = new
            DescriptiveStatistics();

        for (Object obj : responses) {
            if (!(obj instanceof BlockReportingBenchmarkCommand.Response)) {
                throw new IllegalStateException("Wrong response received from the client");
            } else {
                BlockReportingBenchmarkCommand.Response response = (BlockReportingBenchmarkCommand
                    .Response) obj;
                successfulOps.addValue(response.getSuccessfulOps());
                failedOps.addValue(response.getFailedOps());
                speed.addValue(response.getSpeed());
                avgTimePerReport.addValue(response.getAvgTimePerReport());
                avgTimeTogetANewNameNode.addValue(response.getAvgTimeTogetNewNameNode());
            }
        }
        printMasterResultMessages("Successful-Ops: " + successfulOps.getSum()
                + " Failed-Ops: " + failedOps.getSum()
                + " Speed-/sec: " + Math.ceil(speed.getSum())
                + " AvgTimePerReport: " + Math.ceil(avgTimePerReport.getMean())
                + " AvgTimeToGetNameNodeToReport: " + Math.ceil(avgTimeTogetANewNameNode.getMean()));
    }

    private void startInterleavedCommander() throws IOException, ClassNotFoundException {
        System.out.println("Starting Interleaved Benchmark ...");
        prompt();
        InterleavedBenchmarkCommand.Request request =
            new InterleavedBenchmarkCommand.Request(args.getInterleavedCreateFilesPercentage(),
                args.getInterleavedReadFilesPercentage(), args.getInterleavedRenameFilesPercentage(), args.getInterleavedDeleteFilesPercentage(),
                args.getInterleavedStatFilePercentage(), args.getInterleavedStatDirPercentage(),
                args.getInterleavedChmodFilesPercentage(),
                args.getInterleavedMkdirPercentage(),args.getInterleavedBMDuration(), args.getFileSize(),
                args.getReplicationFactor(), args.getBaseDir());
        sendToAllSlaves(request);

        Collection<Object> responses = receiveFromAllSlaves(Integer.MAX_VALUE);
        DescriptiveStatistics successfulOps = new DescriptiveStatistics();
        DescriptiveStatistics failedOps = new DescriptiveStatistics();
        DescriptiveStatistics speed = new DescriptiveStatistics();
        DescriptiveStatistics duration = new DescriptiveStatistics();
        for (Object obj : responses) {
            if (!(obj instanceof InterleavedBenchmarkCommand.Response)) {
                throw new IllegalStateException("Wrong response received from the client");
            } else {
                InterleavedBenchmarkCommand.Response response = (InterleavedBenchmarkCommand.Response) obj;
                successfulOps.addValue(response.getTotalSuccessfulOps());
                failedOps.addValue(response.getTotalFailedOps());
                speed.addValue(response.getOpsPerSec());
                duration.addValue(response.getRunTime());
            }
        }
        printMasterResultMessages("Successful-Ops: " + successfulOps.getSum()
            + " Failed-Ops: " + failedOps.getSum()
            + " Avg-Test-Duration-sec " + Math.ceil(duration.getMean() / 1000)
            + " Speed-/sec: " + Math.ceil(speed.getSum()));
    }

    private void handShakeWithSlaves() throws IOException, ClassNotFoundException {
        //send request
        printMasterLogMessages("Starting Hand Shake Protocol");
        prompt();
        sendHandshakeToAllSlaves(
            new Handshake.Request(args.getSlaveNumThreads(), args.getFileSize(),
                args.getReplicationFactor(), args.getBenchMarkType(),
                args.getBaseDir(),
                args.isEnableRemoteLogging(), args.getRemoteLogginPort(),
                args.getNameNodeRpcAddress(), args.getNameNodeSelectorPolicy(),
                args.getNameNodeRefreshRate()));
        Collection<Object> allResponses = receiveFromAllSlaves(2000);

        for (Object response : allResponses) {
            if (!(response instanceof Handshake.Response)) {
                throw new IllegalStateException("Disobedient slave. Sent me something other than hand shake response");
            }
        }
        printMasterLogMessages("Hand Shanke With All Slave Completed");
    }

    private void warmUpSlaves()
        throws IOException, ClassNotFoundException, SQLException {
        printMasterLogMessages("Warming Up ... ");
        prompt();
        WarmUpCommand.Request warmUpCommand = null;
        if(args.getBenchMarkType() == BenchmarkType.INTERLEAVED || args
            .getBenchMarkType() == BenchmarkType.RAW){
            warmUpCommand = new NamespaceWarmUp.Request(args.getBenchMarkType(), args.getFilesToCreateInWarmUpPhase(), args.getReplicationFactor(),
                args.getFileSize(), args.getBaseDir());
        }else if(args.getBenchMarkType() == BenchmarkType.BR){
            warmUpCommand= new BlockReportingWarmUp.Request(args.getBaseDir()
                , args.getBlockReportingNumOfBlocksPerReport(), args
                .getBlockReportingNumOfBlocksPerFile(), args
                .getBlockReportingNumOfFilesPerDir(), args
                .getReplicationFactor(), args.getBlockReportingMaxBlockSize()
                , args.isBlockReportingSkipCreations(), args.getBlockReportingPersistDatabase());
        }else{
            throw new UnsupportedOperationException("Wrong Benchmark type for" +
                " warm up " + args.getBenchMarkType());
        }

        sendToAllSlaves(warmUpCommand);

        Collection<Object> allResponses = receiveFromAllSlaves(args.getWarmUpPhaseWaitTime());

        for (Object response : allResponses) {
            if (!(response instanceof WarmUpCommand.Response)) {
                throw new IllegalStateException("Disobedient slave. Sent me something other than hand shake response");
            }
        }
        printMasterLogMessages("All Slaves Warmed Up");
    }

    public void startRawBenchmarkPhase(RawBenchmarkCommand.Request request) throws IOException, InterruptedException, ClassNotFoundException {
        printMasterLogMessages("Starting " + request.getPhase() + " using "
                + args.getSlaveNumThreads() * args.getListOfSlaves().size()
                + " client(s). Time phase duration "
                + request.getDurationInMS() / (double) (1000 * 60) + " mins");
        prompt();

        sendToAllSlaves(request);
        printMasterLogMessages(request.getPhase()+" command sent to all slaves. Test duration "+request.getDurationInMS()+" ms");
        Thread.sleep(request.getDurationInMS());
        printMasterLogMessages(request.getPhase()+" waiting to receive responses from the slaves ");
        Collection<Object> responses = receiveFromAllSlaves(10000);

        DescriptiveStatistics successfulOps = new DescriptiveStatistics();
        DescriptiveStatistics failedOps = new DescriptiveStatistics();
        DescriptiveStatistics speed = new DescriptiveStatistics();
        DescriptiveStatistics duration = new DescriptiveStatistics();
        for (Object obj : responses) {
            if (!(obj instanceof RawBenchmarkCommand.Response)
                    || (obj instanceof RawBenchmarkCommand.Response
                    && ((RawBenchmarkCommand.Response) obj).getPhase() != request.getPhase())) {
                throw new IllegalStateException("Wrong response received from the client");
            } else {
                RawBenchmarkCommand.Response response = (RawBenchmarkCommand.Response) obj;
                successfulOps.addValue(response.getTotalSuccessfulOps());
                failedOps.addValue(response.getTotalFailedOps());
                speed.addValue(response.getOpsPerSec());
                duration.addValue(response.getRunTime());
            }
        }
        printMasterResultMessages(request.getPhase() +" " + Math.ceil(speed.getSum()) + " ops/sec. " +
                " Successful-Ops: " + successfulOps.getSum()
                + " Failed-Ops: " + failedOps.getSum()
                + " Avg-Test-Duration-sec " + Math.ceil(duration.getMean() / 1000));
    }

    private void startListener() throws SocketException, UnknownHostException {
        masterSocket = new DatagramSocket(args.getMasterListeningPort(), InetAddress.getByName("0.0.0.0"));
    }

    private void sendHandshakeToAllSlaves(Handshake.Request handshake) throws
        IOException {
        for (int i=0; i< args.getListOfSlaves().size(); i++) {
            InetAddress slave  = args.getListOfSlaves().get(i);
            handshake.setSlaveId(i);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(outputStream);
            os.writeObject(handshake);
            byte[] data = outputStream.toByteArray();
            DatagramPacket packet = new DatagramPacket(data, data.length, slave, args.getSlaveListeningPort());
            masterSocket.send(packet);
        }
    }


    private void sendToAllSlaves(Object obj) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(outputStream);
        os.writeObject(obj);
        byte[] data = outputStream.toByteArray();

        for (InetAddress slave : args.getListOfSlaves()) {
            DatagramPacket packet = new DatagramPacket(data, data.length, slave, args.getSlaveListeningPort());
            masterSocket.send(packet);
            //printMasterMessages("Sent "+obj+" To "+slave);
        }
    }

    private Collection<Object> receiveFromAllSlaves(int timeout) throws IOException, ClassNotFoundException {
        List<Object> responses = new ArrayList<Object>();
        // count responses
        int ack_counter = 0;
        masterSocket.setSoTimeout(timeout);
        while (true) {
            byte[] recvData = new byte[ConfigKeys.BUFFER_SIZE];
            DatagramPacket recvPacket = new DatagramPacket(recvData, recvData.length);
            masterSocket.receive(recvPacket);

            byte[] data = recvPacket.getData();
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is = new ObjectInputStream(in);
            responses.add(is.readObject());

            ack_counter++;
            
            printMasterLogMessages("Received Response Message from "+recvPacket.getAddress().getHostName());
            
            if (ack_counter == args.getListOfSlaves().size()) {
                masterSocket.setSoTimeout(Integer.MAX_VALUE);
                return responses;
            }
        }
    }

    private void prompt() throws IOException {
        if (!args.isSkipAllPrompt()) {
            printMasterLogMessages("Press Enter to start ");
            System.in.read();
        }
    }

    private void printMasterLogMessages(String msg) {
        redColoredText(msg);
    }

    private void printMasterResultMessages(String msg) throws FileNotFoundException, IOException {
        blueColoredText(msg);

        FileWriter out = new FileWriter(args.getResultFile(), true);
        out.write(msg + "\n");
        out.close();
    }

    private void resetResultFile() {
        File file = new File(args.getResultFile());
        if (file.exists()) {
            file.delete();
        }
    }
    
    private void redColoredText(String msg){
        System.out.println((char) 27 + "[31m" + msg);
        System.out.print((char) 27 + "[0m");
    }
    
    private void blueColoredText(String msg){
        System.out.println((char) 27 + "[36m" + msg);
        System.out.print((char) 27 + "[0m");
    }
    
    private void printAllResults() throws FileNotFoundException, IOException{
        System.out.println("\n\n\n");
        System.out.println("************************ All Results ************************");
        System.out.println("\n\n\n");
        BufferedReader br = new BufferedReader(new FileReader(args.getResultFile()));
        try {
            
            String line = br.readLine();

            while (line != null) {
                blueColoredText(line);
                line = br.readLine();
            }
        } finally {
            br.close();
        }
        System.out.println("\n\n\n");
    }
}
