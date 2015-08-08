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
package io.hops.experiments.results;

import io.hops.experiments.controller.ConfigKeys;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author salman
 */
public class CompileResults {

  public static void main(String argv[]) throws FileNotFoundException, IOException, ClassNotFoundException {

    argv = new String[1];
    argv[0] = ".";


    if (argv.length != 1) {
      System.err.println("Usage CompileResults path");
      System.exit(0);
    }

    new CompileResults().doShit(argv[0]);
  }

  private void doShit(String root) throws FileNotFoundException, IOException, ClassNotFoundException {
    List<File> files = findResultFiles(root);
    parseFiles(files);
  }

  private List<File> findResultFiles(String path) {
    List<File> allResultFiles = new ArrayList<File>();
    File root = new File(path);
    if (!root.isDirectory()) {
      System.err.println(path + " is not a directory. Specify a directory that contains all the results");
      System.exit(0);
    }

    List<File> dirs = new ArrayList<File>();
    dirs.add(root);
    while (!dirs.isEmpty()) {
      File dir = dirs.remove(0);

      File[] contents = dir.listFiles();
      for (File content : contents) {
        if (content.isDirectory()) {
          dirs.add(content);

        } else {
          if (content.getAbsolutePath().endsWith(ConfigKeys.BINARY_RESULT_FILE_EXT)) {
            System.out.println("Found a result file  " + content.getAbsolutePath());
            allResultFiles.add(content);
          }
        }
      }
    }
    return allResultFiles;
  }

  private void parseFiles(List<File> files) throws FileNotFoundException, IOException, ClassNotFoundException {

    for (File file : files) {
      System.out.println("Processing File " + file);
      parsetFile(file);


    }


  }
  static int nnCount = -1;

  private void parsetFile(File file) throws FileNotFoundException, IOException, ClassNotFoundException {
    FileInputStream fin = new FileInputStream(file);
    ObjectInputStream ois = new ObjectInputStream(fin);

    Object obj = null;

    try {
      while ((obj = ois.readObject()) != null) {
        if (!(obj instanceof BMResults)) {
          System.out.println("Wrong binary file " + file);
          System.exit(0);
        } else {
          if (nnCount == -1) {
            nnCount = ((BMResults) obj).getNoOfNamenodes();
          }

          if (nnCount == ((BMResults) obj).getNoOfNamenodes()) {
            processObject((BMResults) obj);
          } else {
            System.out.println("The namenode count in all results do not match ");
            System.exit(0);
          }
        }
      }
    } catch (EOFException e) {
    } finally {
      ois.close();
    }

  }

  private void processObject(BMResults result) {
    System.out.println(result);
  }
}
