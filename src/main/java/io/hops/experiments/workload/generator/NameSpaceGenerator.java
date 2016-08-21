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
package io.hops.experiments.workload.generator;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 *
 * @author salman
 */
public class NameSpaceGenerator {
    private int filesPerDirCount;
    private List<String> allDirs;
    private final int FILES_PER_DIR;
    private final int DIR_PER_DIR;
    private int fileCounter;
    private  DirNamesGenerator dirGenerator;
    private String DIR_PREFIX = "hops_dir";
    private String FILE_PREFIX = "hops_file";
    private static Random rand = new Random(System.currentTimeMillis());

    public NameSpaceGenerator(String baseDir, int filesPerDir, int dirPerDir) {
        this.allDirs = new LinkedList<String>();
        this.FILES_PER_DIR = filesPerDir;
        this.DIR_PER_DIR = dirPerDir;
        
        this.fileCounter = 0;
        this.dirGenerator = new DirNamesGenerator(baseDir,DIR_PER_DIR);
        this.filesPerDirCount = 0;
        DIR_PREFIX = rand.nextInt()+DIR_PREFIX;
        FILE_PREFIX = rand.nextInt()+FILE_PREFIX;
    }

    public String generateNewDirPath(){
        String path = dirGenerator.getNextDirName(DIR_PREFIX);
        allDirs.add(path);
        return path;
    }
    
    public String getFileToCreate() {
        
        if(allDirs.isEmpty()){
            generateNewDirPath();
        }
        
        assert filesPerDirCount < FILES_PER_DIR;
        filesPerDirCount++;
        String filePath = allDirs.get(0)+"/"+FILE_PREFIX+"_"+(fileCounter++);
        
        if(filesPerDirCount >= FILES_PER_DIR) {
            allDirs.remove(0);
            filesPerDirCount = 0;
        }
        return filePath;
    }    
}
