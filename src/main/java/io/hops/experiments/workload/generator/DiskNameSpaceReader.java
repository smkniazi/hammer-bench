package io.hops.experiments.workload.generator;

import io.hops.experiments.controller.Logger;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class DiskNameSpaceReader {
  private static DiskNameSpaceReader instance = null;
  private static String instancePath = null;
  private static List<File> list = null;


  public synchronized static DiskNameSpaceReader getInstance(String path) {
    if (instance == null) {
      instance = new DiskNameSpaceReader(path);
      instancePath = path;
    }

    if (instancePath != path) {
      throw new IllegalArgumentException("DiskNameSpaceReader is a singleton." +
              " It can not handle multiple paths");
    }

    return instance;
  }

  private static void readDir(String path) {
    long startTime = System.currentTimeMillis();
    FileUtils files = new FileUtils();
    list = (List<File>) files.listFiles(new File(path), null, true);
    Logger.printMsg("Reading the namespace containing " + list.size() + " files from the disk took " + (System.currentTimeMillis() - startTime) + " ms");
  }

  private DiskNameSpaceReader(String path) {
    readDir(path);
  }

  public synchronized File getFile() throws IOException {
    while (list.size() > 0) {
      File file = list.remove(0);
//            if (file.length() <= (64 * 1024)) {
      return file.getCanonicalFile();
      //           }
    }
    return null;
  }

  public synchronized boolean hasMoreFiles() {
    if (list.size() > 0) {
      return true;
    } else {
      return false;
    }
  }
}
