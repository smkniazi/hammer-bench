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
package io.hops.experiments.results.compiler;

import io.hops.experiments.controller.ConfigKeys;
import io.hops.experiments.results.BMResults;
import io.hops.experiments.results.BlockReportBMResults;
import io.hops.experiments.results.InterleavedBMResults;
import io.hops.experiments.results.RawBMResults;
import io.hops.experiments.results.compiler.RawBMResultAggregator.CompiledResults;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
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
    argv = new String[3];
    argv[0] = "/home/salman/Dropbox/bm-results/2Sep-HDFS-32thds-10000-clients";
    argv[1] = "/home/salman/Dropbox/hops-bm/";///home/salman/bm_wit_cache";    
    argv[2] = "/home/salman/code/hopg/hops-papers/hopsfs-2015/imgs";


    if (argv.length != 3) {
      System.err.println("Usage CompileResults hdfsResults hopsResults outputDir");
      System.exit(0);
    }

    File outputDir = new File(argv[2]);
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }

    new CompileResults().doShit(argv[0], argv[1], argv[2]);
  }

  private void doShit(String hdfsInputDir, String hopsInputDir, String outputDir) throws FileNotFoundException, IOException, ClassNotFoundException {
    RawBMResultAggregator hdfsRawAggregatredResults = new RawBMResultAggregator();
    InterleavedBMResultsAggregator hdfsInterleavedAggregatedResults = new InterleavedBMResultsAggregator();
    BlockReportBMResultsAggregator hdfsBlockReportAggregatedResults = new BlockReportBMResultsAggregator();

    RawBMResultAggregator hopsRawAggregatredResults = new RawBMResultAggregator();
    InterleavedBMResultsAggregator hopsInterleavedAggregatedResults = new InterleavedBMResultsAggregator();
    BlockReportBMResultsAggregator hopsBlockReportAggregatedResults = new BlockReportBMResultsAggregator();


    
    List<File> hdfsResulsFiles = findResultFiles(hdfsInputDir);
    List<File> hopsResulsFiles = findResultFiles(hopsInputDir);
    System.out.println("Processing HDFS Files");
    parseFiles(hdfsResulsFiles, hdfsRawAggregatredResults, hdfsInterleavedAggregatedResults, hdfsBlockReportAggregatedResults);
    System.out.println("Processing Hops Files");
    parseFiles(hopsResulsFiles, hopsRawAggregatredResults, hopsInterleavedAggregatedResults, hopsBlockReportAggregatedResults);
    generateOutputFiles(hdfsRawAggregatredResults, hdfsInterleavedAggregatedResults, hdfsBlockReportAggregatedResults,
            hopsRawAggregatredResults, hopsInterleavedAggregatedResults, hopsBlockReportAggregatedResults, outputDir);
  }

  private List<File> findResultFiles(String path) {
    List<File> allResultFiles = new ArrayList<File>();
    File root = new File(path);
    if (!root.isDirectory()) {
      System.err.println(path + " is not a directory. Specify a directory that contains all the results");
      return allResultFiles;
    }

    List<File> dirs = new ArrayList<File>();
    dirs.add(root);
    while (!dirs.isEmpty()) {
      File dir = dirs.remove(0);

      File[] contents = dir.listFiles();
      if (contents != null && contents.length > 0) {
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
    }
    return allResultFiles;
  }

  private void parseFiles(List<File> files, RawBMResultAggregator rawAggregatredResults,
          InterleavedBMResultsAggregator interleavedAggregatedResults,
          BlockReportBMResultsAggregator blockReportAggregatedResults)
          throws FileNotFoundException, IOException, ClassNotFoundException {
    for (File file : files) {
      System.out.println("Processing File " + file);
      parsetFile(file, rawAggregatredResults, interleavedAggregatedResults, blockReportAggregatedResults);
    }
  }

  private void parsetFile(File file,
          RawBMResultAggregator rawAggregatredResults,
          InterleavedBMResultsAggregator interleavedAggregatedResults,
          BlockReportBMResultsAggregator blockReportAggregatedResults)
          throws FileNotFoundException, IOException, ClassNotFoundException {
    FileInputStream fin = new FileInputStream(file);
    ObjectInputStream ois = new ObjectInputStream(fin);

    Object obj = null;
    try {
      while ((obj = ois.readObject()) != null) {
        if (!(obj instanceof BMResults)) {
          System.out.println("Wrong binary file " + file);
          System.exit(0);
        } else {
          processResult((BMResults) obj, rawAggregatredResults, interleavedAggregatedResults, blockReportAggregatedResults);
        }
      }
    } catch (EOFException e) {
    } finally {
      ois.close();
    }

  }

  private void processResult(BMResults result,
          RawBMResultAggregator rawAggregatredResults,
          InterleavedBMResultsAggregator interleavedAggregatedResults,
          BlockReportBMResultsAggregator blockReportAggregatedResults) {
    if (result instanceof RawBMResults) {
      rawAggregatredResults.processRecord((RawBMResults) result);
    } else if (result instanceof InterleavedBMResults) {
      interleavedAggregatedResults.processRecord((InterleavedBMResults) result);
    } else if (result instanceof BlockReportBMResults) {
      blockReportAggregatedResults.processRecord((BlockReportBMResults) result);
    } else {
      System.err.println("Wrong type of recode read.");
      System.exit(0);
    }
  }

  private void generateOutputFiles(RawBMResultAggregator hdfsRawAggregatredResults,
          InterleavedBMResultsAggregator hdfsInterleavedAggregatedResults,
          BlockReportBMResultsAggregator hdfsBlockReportAggregatedResults,
          RawBMResultAggregator hopsRawAggregatredResults,
          InterleavedBMResultsAggregator hopsInterleavedAggregatedResults,
          BlockReportBMResultsAggregator hopsBlockReportAggregatedResults, 
          String outputDir) throws IOException {

    CompiledResults hdfsRawCr = hdfsRawAggregatredResults.processAllRecords();
    CompiledResults hopsRawCr = hopsRawAggregatredResults.processAllRecords();
    RawBMResultAggregator.combineHDFSandHopsFS(hdfsRawCr, hopsRawCr, outputDir);

    
    InterleavedBMResultsAggregator.combineResults(
            hdfsInterleavedAggregatedResults.getResults(),
            hopsInterleavedAggregatedResults.getResults(),
            outputDir);

    
    BlockReportBMResultsAggregator.combineResults(hdfsBlockReportAggregatedResults.getResults(), 
            hopsBlockReportAggregatedResults.getResults(),
            outputDir);
  }
  
  protected static void writeToFile(String file, String msg, boolean append) throws IOException {
    System.out.println(msg);
    FileWriter out = new FileWriter(file, append);
    out.write(msg);
    out.close();
  }
  
  protected static String format(String val) {
    return String.format("%1$-20s", val);
  }
}
