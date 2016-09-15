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
import io.hops.experiments.benchmarks.BMResult;
import io.hops.experiments.benchmarks.blockreporting.BlockReportBMResults;
import io.hops.experiments.benchmarks.interleaved.InterleavedBMResults;
import io.hops.experiments.benchmarks.rawthroughput.RawBMResults;
import io.hops.experiments.results.compiler.RawBMResultAggregator.CompiledResults;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author salman
 */
public class CompileResults {
  @Option(name = "-hdfs", usage = "Location of result files for HDFS")
  private static String hdfsInputDir = "/non-existant-path" ;

  @Option(name = "-hopsFS", usage = "Location of result files for HopsFS")
  private static String hopsInputDir = "/non-existant-path" ;

  @Option(name = "-output", usage = "Location of output dir")
  private static String outputDir = "/non-existant-path" ;

  @Option(name = "-force", usage = "Also take in to account failed experiments")
  private static boolean force = false;

  public static void main(String argv[]) throws FileNotFoundException, IOException, ClassNotFoundException {
    new CompileResults().doShit(argv);
  }

  private void parseArgs(String[] args) {
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println();
      System.exit(-1);
    }


  }
  private void doShit(String[] argv) throws FileNotFoundException, IOException, ClassNotFoundException {
    parseArgs(argv);

    File dir = new File(outputDir );
    if (!dir.exists()) {
      dir.mkdirs();
    }

    RawBMResultAggregator hdfsRawAggregatredResults = new RawBMResultAggregator();
    InterleavedBMResultsAggregator hdfsInterleavedAggregatedResults = new InterleavedBMResultsAggregator();
    BlockReportBMResultsAggregator hdfsBlockReportAggregatedResults = new BlockReportBMResultsAggregator();
    
    RawBMResultAggregator hopsRawAggregatredResults = new RawBMResultAggregator();
    InterleavedBMResultsAggregator hopsInterleavedAggregatedResults = new InterleavedBMResultsAggregator();
    BlockReportBMResultsAggregator hopsBlockReportAggregatedResults = new BlockReportBMResultsAggregator();

    
    List<File> hdfsResulsFiles = findFiles(hdfsInputDir,ConfigKeys.BINARY_RESULT_FILE_NAME);
    List<File> hopsResulsFiles = findFiles(hopsInputDir,ConfigKeys.BINARY_RESULT_FILE_NAME);
    System.out.println("Processing HDFS Files");
    parseFiles(hdfsResulsFiles, hdfsRawAggregatredResults, hdfsInterleavedAggregatedResults, hdfsBlockReportAggregatedResults);
    System.out.println("Processing Hops Files");
    parseFiles(hopsResulsFiles, hopsRawAggregatredResults, hopsInterleavedAggregatedResults, hopsBlockReportAggregatedResults);
    generateOutputFiles(hdfsRawAggregatredResults, hdfsInterleavedAggregatedResults, hdfsBlockReportAggregatedResults,
            hopsRawAggregatredResults, hopsInterleavedAggregatedResults, hopsBlockReportAggregatedResults, outputDir);
  }

  public static List<File> findFiles(String path, String suffix) {
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
            if (content.getAbsolutePath().endsWith(suffix)) {
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
        if (!(obj instanceof BMResult)) {
          System.out.println("Wrong binary file " + file);
          System.exit(0);
        } else {
          if(!validateResult((BMResult) obj, rawAggregatredResults, interleavedAggregatedResults, blockReportAggregatedResults)){
            System.err.println(file+" Contains Invalid/Inconsistant results");
           if(!force) {
             System.err.println(file+" Will be ignored. ");
             return;
           }
          }
        }
      }
    } catch (EOFException e) {
    }

    //go the begenning of the file.
    ois.close();
    fin.close();
    fin = new FileInputStream(file);
    ois = new ObjectInputStream(fin);

    try {
      while ((obj = ois.readObject()) != null) {
        if (!(obj instanceof BMResult)) {
          System.out.println("Wrong binary file " + file);
          System.exit(0);
        } else {
          processResult((BMResult) obj, rawAggregatredResults, interleavedAggregatedResults, blockReportAggregatedResults);
        }
      }
    } catch (EOFException e) {
    }
    ois.close();
    fin.close();
  }

  private  boolean validateResult(BMResult result,
          RawBMResultAggregator rawAggregatredResults,
          InterleavedBMResultsAggregator interleavedAggregatedResults,
          BlockReportBMResultsAggregator blockReportAggregatedResults) {
    if (result instanceof RawBMResults) {
      return rawAggregatredResults.validate((RawBMResults) result);
    } else if (result instanceof InterleavedBMResults) {
      return interleavedAggregatedResults.validate((InterleavedBMResults) result);
    } else if (result instanceof BlockReportBMResults) {
      return blockReportAggregatedResults.validate((BlockReportBMResults) result);
    } else {
      System.err.println("Wrong type of recode read.");
      System.exit(0);
      return false;
    }
  }

  private void processResult(BMResult result,
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
  
  public static void writeToFile(String file, String msg, boolean append) throws IOException {
    FileWriter out = new FileWriter(file, append);
    out.write(msg);
    out.close();
  }
  
  protected static String format(String val) {
    return String.format("%1$-20s", val);
  }
}
