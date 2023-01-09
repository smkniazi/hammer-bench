/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.hops.experiments.workload.generator;

import java.util.Arrays;

public class DirNamesGenerator {

  private static final int DEFAULT_DIR_PER_DIR = 32;
  private int[] pathIndecies = new int[40]; //level
  private String baseDir;
  private String currentDir;
  private int dirPerDir;

  DirNamesGenerator(String baseDir) {
    this(baseDir, DEFAULT_DIR_PER_DIR);
  }

  DirNamesGenerator(String baseDir, int filesPerDir) {
    this.baseDir = baseDir;
    this.dirPerDir = filesPerDir;
    reset();
  }

  String getNextDirName(String prefix) {
    int depth = 0;
    while (pathIndecies[depth] >= 0) {
      depth++;
    }
    int level;
    for (level = depth - 1;
         level >= 0 && pathIndecies[level] == dirPerDir - 1; level--) {
      pathIndecies[level] = 0;
    }
    if (level < 0) {
      pathIndecies[depth] = 0;
    } else {
      pathIndecies[level]++;
    }
    level = 0;
    String next = baseDir;
    while (pathIndecies[level] >= 0) {
      next = next + "/" + prefix + pathIndecies[level++];
    }
    return next;
  }

  private synchronized void reset() {
    Arrays.fill(pathIndecies, -1);
    currentDir = "";
  }

  synchronized int getFilesPerDirectory() {
    return dirPerDir;
  }

  synchronized String getCurrentDir() {
    return currentDir;
  }
}
