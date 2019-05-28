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

import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
import io.hops.experiments.workload.generator.FileTreeFromDiskGenerator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import io.hops.experiments.workload.generator.FilePool;
import io.hops.experiments.workload.generator.FileTreeGenerator;
import io.hops.experiments.workload.generator.FixeDepthFileTreeGenerator;

public class DFSOperationsUtils {
    private static final boolean READ_WHOLE_FILE = true;
    private static final boolean SERVER_LESS_MODE=false; //only for testing. If enabled then the clients will not
    private static Random rand = new Random(System.currentTimeMillis());
                                                        // contact NNs
    private static ThreadLocal<FileSystem> dfsClients = new ThreadLocal<FileSystem>();
    private static ThreadLocal<FilePool> filePools = new ThreadLocal<FilePool>();

    private static AtomicInteger filePoolCount = new AtomicInteger(0);
    private static AtomicInteger dfsClientsCount = new AtomicInteger(0);

    public static FileSystem getDFSClient(Configuration conf) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return null;
        }
        FileSystem client = dfsClients.get();
        if (client == null) {
            client = (FileSystem) FileSystem.newInstance(conf);
            dfsClients.set(client);
           System.out.println(Thread.currentThread().getName()  +
                " Creating new client. Total: "+ dfsClientsCount.incrementAndGet()+" New Client is: "+client);
        }else{
            System.out.println("Reusing Existing Client "+client);
        }
        return client;
    }

    public static FilePool getFilePool(Configuration conf, String baseDir,
            int dirsPerDir, int filesPerDir, boolean fixedDepthTree, int treeDepth, String fileSizeDistribution,
                                       boolean readFilesFromDisk, String diskFilesPath) {
        FilePool filePool = filePools.get();
        if (filePool == null) {
            if(fixedDepthTree){
              filePool = new FixeDepthFileTreeGenerator(baseDir,treeDepth, fileSizeDistribution);
            }if(readFilesFromDisk){
              filePool = new FileTreeFromDiskGenerator(baseDir,filesPerDir, dirsPerDir,0, diskFilesPath);
            } else{
                filePool = new FileTreeGenerator(baseDir,filesPerDir, dirsPerDir,0, fileSizeDistribution);
            }
            
            filePools.set(filePool);
            System.out.println("New FilePool " +filePool+" created. Total :"+ filePoolCount.incrementAndGet());
        }else{
            System.out.println("Reusing file pool obj "+filePool);
        }
        return filePool;
    }
    
    public static void createFile(FileSystem dfs, String pathStr, short replication, FilePool filePool) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }

        FSDataOutputStream out = dfs.create(new Path(pathStr), replication);
        long size = filePool.getNewFileSize();
        if(size > 0){
            byte[] buffer = new byte[64*1024];
            long read = -1;
            do {
                read = filePool.getFileData(buffer);
                if(read > 0){
                    out.write(buffer, 0, (int)read);
                }
            }while( read > -1);
        }

        out.close();
    }

    public static void readFile(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }

        FSDataInputStream in = dfs.open(new Path(pathStr));
        try {
            byte b;
            do{
                b = in.readByte();
            } while(READ_WHOLE_FILE);
        }catch (EOFException e){
        }finally {
            in.close();
        }
    }

    public static boolean renameFile(FileSystem dfs, Path from, Path to) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return true;
        }
        return dfs.rename(from, to);    
    }

    public static boolean deleteFile(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return true;
        }
        return dfs.delete(new Path(pathStr), true);
    }
    
    public static void ls(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
       dfs.listStatus(new Path(pathStr));
    }
    
    public static void getInfo(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
       dfs.getFileStatus(new Path(pathStr));
    }
    
    public static void chmodPath(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
        dfs.setPermission(new Path(pathStr), new FsPermission((short)0777));
    }
    
    public static void mkdirs(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
        dfs.mkdirs(new Path(pathStr));
    }
    
    public static void chown(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
        dfs.setOwner(new Path(pathStr), System.getProperty("user.name"), System.getProperty("user.name"));
    }
    
    public static void setReplication(FileSystem dfs, String pathStr) throws IOException {
        if(SERVER_LESS_MODE){
            serverLessModeRandomWait();
            return;
        }
        dfs.setReplication(new Path(pathStr), (short)3);
    }
    
    public static String round(double val){
       return String.format("%5s", String.format("%.2f", val));
    }

    public static String format(int spaces, String string) {
        String format = "%1$-" + spaces + "s";
        return String.format(format, string);
    }

    public static boolean isTwoDecimalPlace(double val) {
        if (val == 0 || val == ((int) val)) {
            return true;
        } else {
            String valStr = Double.toString(val);
            int i = valStr.lastIndexOf('.');
            if (i != -1 && (valStr.substring(i + 1).length() == 1 || valStr.substring(i + 1).length() == 2)) {
                return true;
            } else {
                return false;
            }
        }
    }

    public static void appendFile(FileSystem dfs, String pathStr, long size) throws IOException {
        if (SERVER_LESS_MODE) {
            serverLessModeRandomWait();
            return;
        }

        FSDataOutputStream out = dfs.append(new Path(pathStr));
        if (size != 0) {
            for (long bytesWritten = 0; bytesWritten < size; bytesWritten += 1) {
                out.writeByte(1);
            }
        }
        out.close();
    }

    public static int getActiveNameNodesCount(BenchMarkFileSystemName fsName, FileSystem dfs) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (SERVER_LESS_MODE) {
            serverLessModeRandomWait();
            return 0;
        }

        //it only works for HopsFS
        if (fsName == BenchMarkFileSystemName.HopsFS) {
            Class filesystem = dfs.getClass();
            Method method = filesystem.getMethod("getNameNodesCount");
            Object ret = method.invoke(dfs);
            return (Integer) ret;
        } else if (fsName == BenchMarkFileSystemName.HDFS) {
            return 1;
        } else {
            throw new UnsupportedOperationException("Implement get namenode count for other filesystems");
        }
    }

    private static  void serverLessModeRandomWait(){
//        try {
//            Thread.sleep(rand.nextInt(100));
//            Thread.sleep(1);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
}
