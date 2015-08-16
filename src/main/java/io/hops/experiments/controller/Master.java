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
import io.hops.experiments.results.BMResults;
import io.hops.experiments.results.BlockReportBMResults;
import io.hops.experiments.results.InterleavedBMResults;
import io.hops.experiments.results.RawBMResults;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.net.SocketTimeoutException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author salman
 */
public class Master {

    List<BMResults> results = new ArrayList<BMResults>();
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
      try{
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
        
        printAllResults();
        
        generateBinaryFile();

      }finally{
        sendToAllSlaves(new KillSlave());
        System.exit(0);
      }
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
       
        BlockReportBMResults result  = new BlockReportBMResults(args.getNamenodeCount(),
                args.getNoOfNDBDataNodes(),
                speed.getSum(), successfulOps.getSum(), 
                failedOps.getSum(), avgTimePerReport.getMean(), avgTimeTogetANewNameNode.getMean());
        
        printMasterResultMessages(result);
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

        Collection<Object> responses = receiveFromAllSlaves((int)args.getInterleavedBMDuration()+10000);
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
        
        InterleavedBMResults result = new InterleavedBMResults(args.getNamenodeCount(),
                args.getNoOfNDBDataNodes(),
                (speed.getSum()),  (duration.getMean() / 1000),
                (successfulOps.getSum()), (failedOps.getSum()));
        printMasterResultMessages(result);
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
        Collection<Object> allResponses = receiveFromAllSlaves(5000);

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
        
        Collection<Object> responses = receiveFromAllSlaves((int)request.getDurationInMS()+10000);

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
        
        RawBMResults result = new RawBMResults(args.getNamenodeCount(),
                args.getNoOfNDBDataNodes(),
                request.getPhase(),
                (speed.getSum()),  (duration.getMean() / 1000), 
                (successfulOps.getSum()), (failedOps.getSum()) );
        printMasterResultMessages(result);
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

    private Collection<Object> receiveFromAllSlaves(int timeout) throws  ClassNotFoundException, UnknownHostException, IOException {
        List<Object> responses = new ArrayList<Object>();
        // count responses
        int ack_counter = 0;
        List<InetAddress> rcvdFrom = new ArrayList<InetAddress>();
        
        try{
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
            
            rcvdFrom.add(recvPacket.getAddress());
            
            printMasterLogMessages("Received Response Message from "+recvPacket.getAddress().getHostName());
            
            if (ack_counter == args.getListOfSlaves().size()) {
                masterSocket.setSoTimeout(Integer.MAX_VALUE);
                return responses;
            }
        }
        } catch(SocketTimeoutException e){
          //print who missed the response
          printMasterLogMessages("SocketTimeout Exception Received while expecting responses from the slaves ");
          List<InetAddress> allSlaves = args.getListOfSlaves();
          for(InetAddress add : allSlaves){
           if(!rcvdFrom.contains(add)){
             printMasterLogMessages("*** ERROR: "+ add.getCanonicalHostName()+" has not yet responded ");
           }
          }
          throw e;
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

    private void printMasterResultMessages(BMResults result) throws FileNotFoundException, IOException {    
        blueColoredText(result.toString());
        FileWriter out = new FileWriter(args.getResultFile(), true);
        out.write(result.toString() + "\n");
        out.close();
        
        results.add(result);
    }

    private void resetResultFile() {
        File file = new File(args.getResultFile());
        if (file.exists()) {
            file.delete();
        }
        
        file = new File(args.getResultFile()+ConfigKeys.BINARY_RESULT_FILE_EXT);
        if (file.exists()) {
            file.delete();
        }
    }
    
    private void generateBinaryFile() throws FileNotFoundException, IOException{
      FileOutputStream fout = new FileOutputStream(args.getResultFile()+ConfigKeys.BINARY_RESULT_FILE_EXT);
      ObjectOutputStream oos = new ObjectOutputStream(fout);   
      for(BMResults result: results){
        oos.writeObject(result);
      }
      oos.close();
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
