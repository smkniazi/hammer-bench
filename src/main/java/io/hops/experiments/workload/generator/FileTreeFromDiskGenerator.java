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

import io.hops.experiments.benchmarks.common.coin.FileSizeMultiFaceCoin;
import io.hops.experiments.controller.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 *
 * @author salman
 */
public class FileTreeFromDiskGenerator extends FileTreeGenerator {

    private File currentFile = null;
    private FileInputStream inputStream = null;

    DiskNameSpaceReader diskNameSpaceReader = null;

    public FileTreeFromDiskGenerator(String baseDir, int filesPerDir,
                                     int dirPerDir, int initialTreeDepth) {
        super(baseDir, filesPerDir, dirPerDir, initialTreeDepth, null);
        diskNameSpaceReader = DiskNameSpaceReader.getInstance("/home/salman");
    }

    @Override
    public long getFileData(byte[] buffer) throws IOException {
        if(inputStream != null){
            return  inputStream.read(buffer);
        } else {
            return -1;
        }
    }


    @Override
    public long getNewFileSize() throws IOException {
        if(inputStream != null){
           //close the old file
            inputStream.close();
        }
        currentFile = diskNameSpaceReader.getFile();
        if (currentFile != null) {
            System.out.println("File: " + currentFile.getCanonicalPath() + " Size: " + currentFile.length());
            inputStream = new FileInputStream(currentFile.getAbsoluteFile());
            return currentFile.length();
        } else {
            inputStream = null;
            currentFile = null;
            return 0;
        }


    }
}

