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
package io.hops.experiments.utils;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import io.hops.experiments.workload.generator.FilePool;
import io.hops.experiments.workload.generator.TreeFileGenerator;

public class BenchmarkUtils {

    private static ThreadLocal<DistributedFileSystem> dfsClients = new ThreadLocal<DistributedFileSystem>();
    private static ThreadLocal<FilePool> filePools = new ThreadLocal<FilePool>();
    
    private static int filePoolCount = 0;
    private static int dfsClientsCount = 0;

    public static DistributedFileSystem getDFSClient(Configuration conf) throws IOException {
        DistributedFileSystem client = dfsClients.get();
        if (client == null) {
            System.out.println(Thread.currentThread().getName()  +
                " Creating new client. Total: "+ ++dfsClientsCount);
            client = (DistributedFileSystem) FileSystem.newInstance(conf);
            dfsClients.set(client);
        }else{
            System.out.println("Reusing Existing Client "+client);
        }
        return client;
    }

    public static FilePool getFilePool(Configuration conf, String baseDir, int dirsPerDir, int filesPerDir) {
        FilePool filePool = filePools.get();
        if (filePool == null) {
            filePool = new TreeFileGenerator(baseDir,filesPerDir, dirsPerDir,0);
            filePools.set(filePool);
            System.out.println("New FilePool created. Total :"+ ++filePoolCount);
        }else{
            //System.out.println("Reusing file pool obj");
        }
        
        return filePool;
    }
    
    public static void createFile(DistributedFileSystem dfs, Path path, short replication, final long size /*in bytes*/) throws IOException {
        FSDataOutputStream out = dfs.create(path, replication);
        if (size != 0) {
            for (long bytesWritten = 0; bytesWritten < size; bytesWritten += 4) {
                out.writeInt(1);
            }
        }
        out.close();
    }

    public static void readFile(DistributedFileSystem dfs, Path path, final long size /*in bytes*/) throws IOException {
        FSDataInputStream in = dfs.open(path);
//        if (size != 0) {
//            for (long bytesRead = 0; bytesRead < size; bytesRead += 4) {
//                in.readInt();
//            }
//        }
        in.close();
    }

    public static boolean renameFile(DistributedFileSystem dfs, Path from, Path to) throws IOException {
        return dfs.rename(from, to);    
    }

    public static boolean deleteFile(DistributedFileSystem dfs, Path file) throws IOException {
        return dfs.delete(file, true);
    }
    
    public static void ls(DistributedFileSystem dfs, Path path) throws IOException {
       dfs.listStatus(path);
    }
    
    public static void getInfo(DistributedFileSystem dfs, Path path) throws IOException {
       dfs.getFileStatus(path);
    }
    
    public static void chmodPath(DistributedFileSystem dfs, Path path) throws IOException {
        dfs.setPermission(path, new FsPermission((short)0777));
    }
    
    public static void mkdirs(DistributedFileSystem dfs, Path path) throws IOException {
        dfs.mkdirs(path);
    }
    
    public static void chown(DistributedFileSystem dfs, Path path) throws IOException {     
        dfs.setOwner(path, System.getProperty("user.name"), System.getProperty("user.name"));
    }
    
    public static void setReplication(DistributedFileSystem dfs, Path path) throws IOException {
        dfs.setReplication(path, (short)3);
    }
    
    public static double round(double val){
      double round = val * 100;
      round = Math.ceil(round);
      return round / 100;
    }

  public static void appendFile(DistributedFileSystem dfs, Path path, long size) throws IOException {
    FSDataOutputStream out = dfs.append(path);
        if (size != 0) {
            for (long bytesWritten = 0; bytesWritten < size; bytesWritten += 4) {
                out.writeInt(1);
            }
        }
        out.close();
  }
}
