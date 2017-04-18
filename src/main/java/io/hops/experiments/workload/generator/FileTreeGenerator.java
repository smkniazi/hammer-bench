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
package io.hops.experiments.workload.generator;

import io.hops.experiments.controller.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 *
 * @author salman
 */
public class FileTreeGenerator implements FilePool {

  private Random rand1;
  private UUID uuid = null;
  protected List<String> allThreadFiles;
  protected List<String> allThreadDirs;
  protected String threadDir;
  private NameSpaceGenerator nameSpaceGenerator;
  private final int THRESHOLD = 3;
  private int currIndex = -1;

  public FileTreeGenerator(String baseDir, int filesPerDir,
          int dirPerDir, int initialTreeDepth) {

    this.allThreadFiles = new ArrayList<String>(1000000);
    this.allThreadDirs = new ArrayList<String>(1000000);
    this.rand1 = new Random(System.currentTimeMillis());
    uuid = UUID.randomUUID();


    String machineName = "";
    try {
      machineName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      machineName = "Client_Machine+" + rand1.nextInt();
    }

    baseDir = baseDir.trim();
    if (!baseDir.endsWith("/")) {
      baseDir = baseDir + "/";
    }

    if(baseDir.compareTo("/")==0){
      threadDir = baseDir + machineName+"_"+uuid;
    }else{
      threadDir = baseDir + machineName+"/"+uuid;
    }

    String[] comp = PathUtils.getPathNames(threadDir);

    int more = 0;
    if (initialTreeDepth - comp.length > 0) {
      more = initialTreeDepth - comp.length;
      for (int i = comp.length; i < (initialTreeDepth); i++) {
        threadDir += "/added_depth_" + i;
      }
    }

    nameSpaceGenerator = new NameSpaceGenerator(threadDir, filesPerDir, dirPerDir);
  }

  @Override
  public String getDirToCreate() {
    String path = nameSpaceGenerator.generateNewDirPath();
    allThreadDirs.add(path);
    return path;
  }

  @Override
  public String getFileToCreate() {
    String path = nameSpaceGenerator.getFileToCreate();
    return path;
  }

  @Override
  public void fileCreationSucceeded(String file) {
    allThreadFiles.add(file);
  }

  @Override
  public String getFileToRead() {
    return getRandomFile();
  }

  @Override
  public String getFileToRename() {
    if (allThreadFiles.isEmpty()) {
      return null;
    }

    for (int i = 0; i < allThreadFiles.size(); i++) {
      currIndex = rand1.nextInt(allThreadFiles.size());
      String path = allThreadFiles.get(currIndex);
      if (getPathLength(path) < THRESHOLD) {
        continue;
      }
      //System.out.println("Rename path "+path);
      return path;
    }

    return null;
  }

  @Override
  public void fileRenamed(String from, String to) {
    String curr = allThreadFiles.get(currIndex);
    if(curr != from){
      IllegalStateException up = new IllegalStateException("File name did not match.");
      throw up;
    }
    allThreadFiles.set(currIndex, to);
  }

  @Override
  public String getFileToDelete() {
    if (allThreadFiles.isEmpty()) {
      return null;
    }
    currIndex = rand1.nextInt(allThreadFiles.size());
    for (int i = 0; i < allThreadFiles.size(); i++) {
      String file = allThreadFiles.remove(currIndex);
      if(getPathLength(file) < THRESHOLD){
        continue;
      }
      //System.out.println("Delete Path "+file);
      return file;
    }
    return null;
  }

  @Override
  public String getDirToStat() {
    return getRandomDir();
  }

  @Override
  public String getFileToStat() {
    return getRandomFile();
  }

  @Override
  public String getFilePathToChangePermissions() {
    return getRandomFile();
  }

  @Override
  public String getDirPathToChangePermissions() {
    return getRandomDir();
  }

  @Override
  public String getFileToInfo() {
    return getRandomFile();
  }

  @Override
  public String getDirToInfo() {
    return getRandomDir();
  }

  @Override
  public String getFileToSetReplication() {
    return getRandomFile();
  }

  @Override
  public String getFileToAppend() {
    return getRandomFile();
  }

  @Override
  public String getFileToChown() {
    return getRandomFile();
  }

  @Override
  public String getDirToChown() {
    return getRandomDir();
  }

  private String getRandomFile() {
    if (!allThreadFiles.isEmpty()) {
      for (int i = 0; i < allThreadFiles.size(); i++) {
        currIndex = rand1.nextInt(allThreadFiles.size());
        String path = allThreadFiles.get(currIndex);
        if (getPathLength(path) < THRESHOLD) {
          continue;
        }
//        System.out.println("Path "+path);
        return path;
      }
    }

    System.err.println("Unable to getRandomFile from file pool: "+this+" PoolSize is: "+allThreadFiles.size());
    Logger.printMsg("Error: Unable to getRandomFile from file pool: "+this+" PoolSize is: "+allThreadFiles.size());
    return null;
  }

  private int getPathLength(String path){
    return PathUtils.getPathNames(path).length;
  }

  public String getRandomDir() {
    if (!allThreadFiles.isEmpty()) {
      for (int i = 0; i < allThreadFiles.size(); i++) {
        currIndex = rand1.nextInt(allThreadFiles.size());
        String path = allThreadFiles.get(currIndex);
        int dirIndex = path.lastIndexOf("/");
        path = path.substring(0, dirIndex);
        if (getPathLength(path) < THRESHOLD) {
          continue;
        }
//        System.out.println("Path "+path+ " after retires: "+i);
        return path;
      }
    }

    System.err.println("Unable to getRandomDir from file pool: "+this+" PoolSize is: "+allThreadFiles.size());
    Logger.printMsg("Error: Unable to getRandomDir from file pool: "+this+" PoolSize is: "+allThreadFiles.size());
    return null;
  }
}

