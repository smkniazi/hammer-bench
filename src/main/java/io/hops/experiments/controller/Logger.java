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
package io.hops.experiments.controller;

import io.hops.experiments.benchmarks.common.config.ConfigKeys;
import io.hops.experiments.utils.DFSOperationsUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 *
 * @author salman
 */
public class Logger {

  static long lastmsg = 0;
  private static InetAddress loggerIp = null;
  private static int loggerPort = 0;
  private static boolean enableRemoteLogging = false;
  private static DatagramSocket socket = null;

  //send one error per sec. this avoids printing too much to the output of the master
  static long lastError = 0;
  static long errorCounter = 0;
  public static synchronized void error(Throwable e) {
    e.printStackTrace();
    if(((System.currentTimeMillis() - lastError) > 2000)){
      final int MSG_SIZE = 200; //send small messages
      String msg = e.getClass().getName() + " ";
      int consumed = msg.length();
      if (e.getMessage().length() > (MSG_SIZE - consumed)) {
        msg += e.getMessage().substring(0, (MSG_SIZE - consumed));
        msg += " ... ";
      }

      printMsg(errorCounter+" errors since last error. New error is: "+msg);
      errorCounter=0;
      lastError = System.currentTimeMillis();
    }else{
      errorCounter++;
    }

  }

  public static void resetTimer(){
    lastmsg = System.currentTimeMillis();
  }

 public static synchronized void printMsg(String msg) {
    if (enableRemoteLogging && msg.length() > 0 ) {
      try {
        if (socket == null) {
          socket = new DatagramSocket();
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(outputStream);
        os.writeObject(msg);
        byte[] data = outputStream.toByteArray();

        DatagramPacket packet = new DatagramPacket(data, data.length,
                loggerIp, loggerPort);
        socket.send(packet);
        System.out.println(msg);
        os.close();
        outputStream.close();
      } catch (Exception e) { // logging should not crash the client 
        e.printStackTrace();
      }
    }
  }

  public static synchronized boolean canILog() {
    if ((System.currentTimeMillis() - lastmsg) > 5000) {
      lastmsg = System.currentTimeMillis();
      return true;
    } else {
      return false;
    }
  }

  public static void setLoggerIp(InetAddress loggerIp) {
    System.out.println("Remote Logger IP: "+loggerIp);
    Logger.loggerIp = loggerIp;
  }

  public static void setLoggerPort(int loggerPort) {
    System.out.println("Remote Logger Port: "+loggerPort);
    Logger.loggerPort = loggerPort;
  }

  public static void setEnableRemoteLogging(boolean enableRemoteLogging) {
    Logger.enableRemoteLogging = enableRemoteLogging;
  }

  public static class LogListener implements Runnable {

    private int port;
    private boolean running = true;
    private Map<String, Double> speedMap = new HashMap<String, Double>();
    private final int maxSlaves;
    private long firstAggSpeed = 0;
    private long lastAggSpeed = 0;
    
    public LogListener(int port, int maxSlaves) {
      this.port = port;
      this.maxSlaves = maxSlaves;
    }

    @Override
    public void run() {

      try {
        socket = new DatagramSocket(port, InetAddress.getByName("0.0.0.0"));
      } catch (Exception e) {
        e.printStackTrace();
      }
      while (running) {
        DatagramPacket recvPacket = null;
        try {
          byte[] recvData = new byte[ConfigKeys.BUFFER_SIZE];
          recvPacket = new DatagramPacket(recvData, recvData.length);
          socket.receive(recvPacket);
          ByteArrayInputStream in = new ByteArrayInputStream(recvData);
          ObjectInputStream is = new ObjectInputStream(in);
          String msg = (String) is.readObject();
          System.out.println(DFSOperationsUtils.format(20,recvPacket.getAddress().getHostName()+" -> ") + msg);
          continuousAggSpeed(recvPacket.getAddress().getHostName(), msg);
          is.close();
          in.close();
          recvPacket = null;
        } catch (Exception e) { // Logger should not crash the application
          if(recvPacket != null){
            System.out.println("Exception when receiving from "+recvPacket.getAddress().getHostName()+" "+e);
          }
        }
      }
    }

    public synchronized void continuousAggSpeed(String address, String msg){
      try{
        String token = "Speed:";
        if(msg.contains(token)){
          StringTokenizer st = new StringTokenizer(msg, " ");
          while(st.hasMoreTokens()){
            String t = st.nextToken();
            if (t.compareToIgnoreCase(token)==0){
              String speedStr = st.nextToken();
              Double speed = Double.parseDouble(speedStr);
              speedMap.put(address, speed);
              break;
            }
          }
        }

        if(firstAggSpeed == 0){
          firstAggSpeed = System.currentTimeMillis();
        }
  
        if(lastAggSpeed == 0){
          lastAggSpeed = System.currentTimeMillis();
        }
        
        int seconds =
            (int) Math.floor(
                (System.currentTimeMillis() - firstAggSpeed) / 1000.0);
        
        
        if(speedMap.size() == maxSlaves ){
          double aggSpeed = 0;
          for(Double speed: speedMap.values()){
            aggSpeed += speed;
          }
          Master.blueColoredText(seconds + " : Current Aggregated Speed is : "+aggSpeed + " : " + maxSlaves);
          speedMap.clear();
        }
      }catch(NumberFormatException e){
      }
 }

    public void stop() {
      this.running = false;
    }
  }
}
