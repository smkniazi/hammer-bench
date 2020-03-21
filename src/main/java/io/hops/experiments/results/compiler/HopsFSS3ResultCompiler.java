/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
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
package io.hops.experiments.results.compiler;

import com.google.common.io.Files;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.benchmarks.common.config.ConfigKeys;
import io.hops.experiments.benchmarks.rawthroughput.RawBMResults;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HopsFSS3ResultCompiler {
  final static String[] DATAPOINTS = {"1-kb", "8-kb", "64-kb", "128-kb", "256-kb",
      "1-mb", "32-mb", "64-mb"};
  
  public static void main(String[] args) throws Exception {
    File baseDir = new File("/Volumes/Data/src/hops-papers/hopsfs/hopsfs-s3" +
        "/results");
    //processRaw(baseDir);
    processPercentiles(baseDir);
    System.exit(0);
  }
  
  
  private static void processRaw(File baseDir) throws Exception{
    File hopsfsDir = new File(baseDir,"hopsfs");
    File S3Dir = new File(baseDir,"S3/5-nodes");
  
    Map<String, Map<BenchmarkOperations, Double>> s3Results = processS3(S3Dir);
  
    Map<String, Map<BenchmarkOperations, RawAggregate>> hopsfsWithSmallFiles
        = process(new File(hopsfsDir, "baseline/withdata-small-files-enabled"));
  
    Map<String, Map<BenchmarkOperations, RawAggregate>> hopsfsNoSmallFiles =
        process(new File(hopsfsDir, "baseline/withdata-small-files-disabled"));
  
    Map<String, Map<BenchmarkOperations, RawAggregate>> hopsfsS3WithCache =
        process(new File(hopsfsDir, "hopsfs-s3/cache-enabled"));
  
    Map<String, Map<BenchmarkOperations, RawAggregate>> hopsfsS3WithNoCache =
        process(new File(hopsfsDir, "hopsfs-s3/cache-disabled"));
  
    List<Map<String, Map<BenchmarkOperations, RawAggregate>>> results =
        Arrays.asList(hopsfsNoSmallFiles, hopsfsWithSmallFiles,
            hopsfsS3WithNoCache, hopsfsS3WithCache);
  
    StringBuffer sbRead = new StringBuffer();
    StringBuffer sbWrite = new StringBuffer();
    sbRead.append("#data S3 HopsFS-no-sm HopsFS-sm HopsFS-S3-no-c " +
        "HopsFS-S3-c\n");
    sbWrite.append("#data S3 HopsFS-no-sm HopsFS-sm HopsFS-S3-no-c " +
        "HopsFS-S3-c\n");
  
    for(String datapoint : DATAPOINTS){
      sbRead.append(datapoint.toUpperCase().replaceAll("-", "") + " ");
      sbWrite.append(datapoint.toUpperCase().replaceAll("-", "") + " ");
    
      Map<BenchmarkOperations, Double> s3Data = s3Results.get(datapoint);
      if(s3Data != null){
        sbWrite.append(s3Data.get(BenchmarkOperations.CREATE_FILE) + " ");
        sbRead.append(s3Data.get(BenchmarkOperations.READ_FILE) + " ");
      }else{
        sbWrite.append("- ");
        sbRead.append("- ");
      }
    
      for(Map<String, Map<BenchmarkOperations, RawAggregate>> result : results){
        Map<BenchmarkOperations, RawAggregate> aggMap = result.get(datapoint);
        if(aggMap != null){
          sbWrite.append(aggMap.get(BenchmarkOperations.CREATE_FILE).getSpeed() + " ");
          sbRead.append(aggMap.get(BenchmarkOperations.READ_FILE).getSpeed() + " ");
        }else{
          sbWrite.append("- ");
          sbRead.append("- ");
        }
      }
      sbRead.append("\n");
      sbWrite.append("\n");
    }
  
    System.out.println("READ.dat");
    System.out.println(sbRead.toString());
  
    System.out.println();
    System.out.println();
  
    System.out.println("Write.dat");
    System.out.println(sbWrite.toString());
  }
  
  private static Map<String, Map<BenchmarkOperations, RawAggregate>> process(File baseDir) throws Exception{
    Map<String, Map<BenchmarkOperations, RawAggregate>> results = new HashMap<String, Map<BenchmarkOperations, RawAggregate>>();
    for(String datapoint : DATAPOINTS){
      File dir = new File(baseDir, datapoint);
      if(dir.exists()){
        List<File> runs = CompileResults.findFiles(dir.getAbsolutePath(),
            ConfigKeys.BINARY_RESULT_FILE_NAME);
        results.put(datapoint, processRuns(runs));
      }
    }
    return results;
  }
  
  private static Map<BenchmarkOperations, RawAggregate> processRuns(List<File> runFiles) throws Exception{
    List<RawBMResults> results = new ArrayList<RawBMResults>();
    for(File run : runFiles){
      results.addAll(parseFile(run));
    }
  
    Map<BenchmarkOperations, RawAggregate> aggregateMap = new HashMap<BenchmarkOperations, RawAggregate>();
    for(RawBMResults result : results){
      RawAggregate agg = aggregateMap.get(result.getOperationType());
      if (agg == null) {
        agg = new RawAggregate();
        aggregateMap.put(result.getOperationType(), agg);
      }
      agg.addSpeed(result.getSpeed());
    }
    return aggregateMap;
  }
  
  private static List<RawBMResults> parseFile(File run) throws Exception{
    FileInputStream fin = new FileInputStream(run);
    ObjectInputStream ois = new ObjectInputStream(fin);
    Object obj1 =  ois.readObject();
    Object obj2 = ois.readObject();
    return Arrays.asList((RawBMResults) obj1, (RawBMResults) obj2);
  }
  
  private static Map<String, Map<BenchmarkOperations, Double>> processS3(File S3Dir) throws Exception{
    Map<String,Map<BenchmarkOperations, Double>> results = new HashMap<String, Map<BenchmarkOperations, Double>>();
    for(String datapoint : DATAPOINTS){
      File run = new File(S3Dir, datapoint + ".txt");
      if(run.exists()){
        List<String> lines = Files.readLines(run, Charset.defaultCharset());
        double puts = 0;
        double gets = 0;
        for(String line : lines){
          String l = line.trim();
          if(l.isEmpty() || l.startsWith("Run")){
            continue;
          }
          
          if(l.startsWith("Test: PUT")){
            String[] slines = l.split(" ");
            puts += Double.valueOf(slines[6]);
          }
  
          if(l.startsWith("Test: GET")){
            String[] slines = l.split(" ");
            gets += Double.valueOf(slines[6]);
          }
        }
        Map<BenchmarkOperations, Double> runResults = new HashMap<BenchmarkOperations, Double>();
        runResults.put(BenchmarkOperations.CREATE_FILE, (puts/2));
        runResults.put(BenchmarkOperations.READ_FILE, (gets/2));
        results.put(datapoint, runResults);
      }
    }
    return results;
  }
  
  private static void processPercentiles(File baseDir) throws Exception{
    File percentilesDir = new File(baseDir, "hopsfs/percentiles");
    List<String> subdirs = Arrays.asList("hopsfs-base", "hopsfs-s3-cache" +
        "-enabled");
    List<String> opDirs = Arrays.asList("read-op", "write-op");
    for(String s : subdirs){
      for(String op: opDirs){
        File dir = new File(percentilesDir, s + "/" + op);
        File[] exps = dir.listFiles();
        for(File exp : exps){
          if(exp.getName().startsWith(".")){
            continue;
          }
          File source = new File(exp, "run_1");
          File dst =new File(exp, "out");
          dst.mkdirs();
          System.out.println("Calculate percentiles for " + exp);
          new CalculatePercentiles().doShit(source.getAbsolutePath(), dst.getAbsolutePath(),
              exp.getName(), 5);
        }
      }
    }
  }
}
