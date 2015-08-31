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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 *
 * @author salman
 */
public class TreeFileGenerator implements FilePool {

  private Random rand;
  private UUID uuid = null;
  protected List<String> allThreadFiles;
  protected List<String> allThreadDirs;
  protected String threadDir;
  private NameSpaceGenerator nameSpaceGenerator;

  public TreeFileGenerator(String baseDir, int filesPerDir, int dirPerDir, int addedDepth) {
    this.allThreadFiles = new LinkedList<String>();
    this.allThreadDirs = new LinkedList<String>();
    this.rand = new Random(System.currentTimeMillis());
    uuid = UUID.randomUUID();


    String machineName = "";
    try {
      machineName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      machineName = "Client_Machine+" + rand.nextInt();
    }

    if (!baseDir.endsWith("/")) {
      threadDir = baseDir + "/";
    }
    threadDir = threadDir + "_" + machineName + "_" + uuid;

    String[] comp = PathUtils.getPathNames(threadDir);

    if (comp.length < addedDepth) {
      for (int i = comp.length; i < (addedDepth); i++) {
        threadDir += "/depth_" + i;
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
    allThreadFiles.add(path);
    return path;
  }

  @Override
  public void fileCreationFailed(String file) {
    allThreadFiles.remove(file);
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
    int renameIndex = rand.nextInt(allThreadFiles.size());
    String path = allThreadFiles.get(renameIndex);
    //System.out.println("Rename path "+path);
    return path;
  }

  @Override
  public void fileRenamed(String from, String to) {
    int index = allThreadFiles.indexOf(from);
    allThreadFiles.remove(index);
    allThreadFiles.add(index, to);
  }

  @Override
  public String getFileToDelete() {
    if (allThreadFiles.isEmpty()) {
      return null;
    }
    int deleteIndex = rand.nextInt(allThreadFiles.size());
    String file = allThreadFiles.remove(deleteIndex);
    //System.out.println("Delete Path "+file);
    return file;
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

  private String getRandomFile() {
    if (allThreadFiles.isEmpty()) {
      return null;
    }
    int index = rand.nextInt(allThreadFiles.size());
    String path = allThreadFiles.get(index);
    //System.out.println("Chmod Path "+path);
    return path;
  }

  public String getRandomDir() {
    if (allThreadFiles.isEmpty()) {
      return null;
    }
    int index = rand.nextInt(allThreadFiles.size());
    String path = allThreadFiles.get(index);
    //System.out.println("Chmod Path "+path);
    return path;
  }
}
