/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.experiments.benchmarks;

import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.utils.BenchmarkUtils;
import io.hops.experiments.workload.generator.FilePool;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author salman
 */
public class OperationsUtils {
  public static String getPath(BenchmarkOperations opType, FilePool filePool) {
      String path = null;
      if (opType == BenchmarkOperations.SET_REPLICATION) {
        path = filePool.getFileToSetReplication();
      } else if (opType == BenchmarkOperations.FILE_INFO) {
        path = filePool.getFileToInfo();
      } else if (opType == BenchmarkOperations.DIR_INFO) {
        path = filePool.getDirToInfo();
      } else if (opType == BenchmarkOperations.CHMOD_DIR) {
        path = filePool.getDirPathToChangePermissions();
      } else if (opType == BenchmarkOperations.CHMOD_FILE) {
        path = filePool.getFilePathToChangePermissions();
      } else if (opType == BenchmarkOperations.LS_FILE) {
        path = filePool.getFileToStat();
      } else if (opType == BenchmarkOperations.LS_DIR) {
        path = filePool.getDirToStat();
      } else if (opType == BenchmarkOperations.READ_FILE) {
        path = filePool.getFileToRead();
      } else if (opType == BenchmarkOperations.MKDIRS) {
        path = filePool.getDirToCreate();
      } else if (opType == BenchmarkOperations.CREATE_FILE) {
        path = filePool.getFileToCreate();
      } else if (opType == BenchmarkOperations.DELETE_FILE) {
        path = filePool.getFileToDelete();
      } else if (opType == BenchmarkOperations.RENAME_FILE) {
        path = filePool.getFileToRename();
      } else if (opType == BenchmarkOperations.APPEND_FILE) {
        path = filePool.getFileToAppend();
      } else if (opType == BenchmarkOperations.CHOWN_FILE) {
        path = filePool.getFileToChown();
      } else if (opType == BenchmarkOperations.CHOWN_DIR) {
        path = filePool.getDirToChown();
      }
      else{
        throw new IllegalStateException("Fucked");
      }
      
      //System.out.println(opType+" Path: "+path);
      return path;
    }

    static final String RENAMED = "RN";
    public static boolean performOp(FileSystem dfs, BenchmarkOperations opType,
            FilePool filePool, String path, short replicationFactor,
            long fileSize, long appendSize) throws IOException {
      if (opType == BenchmarkOperations.SET_REPLICATION) {
        BenchmarkUtils.setReplication(dfs, path);
      } else if (opType == BenchmarkOperations.FILE_INFO
              || opType == BenchmarkOperations.DIR_INFO) {
        BenchmarkUtils.getInfo(dfs, path);
      } else if (opType == BenchmarkOperations.CHMOD_FILE
              || opType == BenchmarkOperations.CHMOD_DIR) {
        BenchmarkUtils.chmodPath(dfs, path);
      } else if (opType == BenchmarkOperations.LS_FILE
              || opType == BenchmarkOperations.LS_DIR) {
        BenchmarkUtils.ls(dfs, path);
      } else if (opType == BenchmarkOperations.READ_FILE) {
        BenchmarkUtils.readFile(dfs, path);
      } else if (opType == BenchmarkOperations.MKDIRS) {
        BenchmarkUtils.mkdirs(dfs, path);
      } else if (opType == BenchmarkOperations.CREATE_FILE) {
          BenchmarkUtils.createFile(dfs, path, replicationFactor, fileSize);
          filePool.fileCreationSucceeded(path);
      } else if (opType == BenchmarkOperations.DELETE_FILE) {
        BenchmarkUtils.deleteFile(dfs, path);
      } else if (opType == BenchmarkOperations.RENAME_FILE) {
        String from = path;
//        Count the number of times a files has been renamed
        int currentCounter = 0;
        String to = from;
        if (from.contains(RENAMED)) {
          int index1 = from.lastIndexOf(RENAMED);
          int index2 = from.lastIndexOf("_");
          String counter = from.substring(index1 + RENAMED.length() + 1, index2);
          to = from.substring(0, index1 - 1);
          currentCounter = Integer.parseInt(counter);
        }
        currentCounter++;
        to = to + "_" + RENAMED + "_" + currentCounter + "_" + "Times";
//        String to = from;
        if (BenchmarkUtils.renameFile(dfs, new Path(from), new Path(to))) {
          filePool.fileRenamed(from, to);
        }
      }else if (opType == BenchmarkOperations.APPEND_FILE) {
        BenchmarkUtils.appendFile(dfs, path,appendSize);
      } else if (opType == BenchmarkOperations.CHOWN_DIR ||
              opType == BenchmarkOperations.CHOWN_FILE) {
        BenchmarkUtils.chown(dfs, path);
      }
      
      else {
        throw new IllegalStateException("Fucked. "+opType);
      }
      return true;
    }
  
  
}
