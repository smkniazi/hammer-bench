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

import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.results.RawBMResults;
import io.hops.experiments.utils.BenchmarkUtils;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author salman
 */
public class RawBMResultAggregator {

  Map<Integer/*NN Count*/, Map<BenchmarkOperations, RawAggregate/*aggregates*/>> allResults =
          new HashMap<Integer, Map<BenchmarkOperations, RawAggregate>>();

  private static final String RECORD_NOT_FOUND = "-";
  public RawBMResultAggregator() {
    
  }

  public void processRecord(RawBMResults result) {
    // System.out.println(result);

    Map<BenchmarkOperations, RawAggregate> map = allResults.get(result.getNoOfNamenodes());

    if (map == null) {
      map = new HashMap<BenchmarkOperations, RawAggregate>();
      allResults.put(result.getNoOfNamenodes(), map);
    }

    RawAggregate agg = map.get(result.getOperationType());
    if (agg == null) {
      agg = new RawAggregate();
      map.put(result.getOperationType(), agg);
    }

    agg.addSpeed(result.getSpeed());
    agg.addFailedOps(result.getFailedOps());
    agg.addSucessfulOps(result.getSuccessfulOps());
    agg.addRunDuration(result.getDuration());
  }

  CompiledResults processAllRecords() {
    CompiledResults cr = new CompiledResults();

    System.out.println("Generating compiled results for RAW Benchmarks");
    if (allResults.isEmpty()) {
      return cr;
    }

    for (Integer key : allResults.keySet()) {
      cr.nnCounts.add(key);
      Map<BenchmarkOperations, RawAggregate> map = allResults.get(key);
      for (BenchmarkOperations op : map.keySet()) {
        RawAggregate agg = map.get(op);
        List<Double> vals = cr.valsMap.get(op);
        if (vals == null) {
          vals = new ArrayList<Double>();
          cr.valsMap.put(op, vals);
        }
        vals.add(agg.getSpeed());
      }
    }

    //create histogram
    for (BenchmarkOperations op : cr.valsMap.keySet()) {
      List<Double> vals = cr.valsMap.get(op);
      List<Double> histo = cr.histoMap.get(op);
      if (histo == null) {
        histo = new ArrayList<Double>();
        cr.histoMap.put(op, histo);
      }
      Double previousVal = null;
      for (Double val : vals) {
        double diff = 0;
        if (previousVal == null) {
          previousVal = val;
          diff = val;
        } else {
          diff = (val - previousVal);
          previousVal = val;
        }
        histo.add(diff);
      }
    }

    return cr;
  }

  private static String format(String val) {
    return String.format("%1$-20s", val);
  }

  class CompiledResults {

    Map<BenchmarkOperations, List<Double>> valsMap = new HashMap<BenchmarkOperations, List<Double>>();
    Map<BenchmarkOperations, List<Double>> histoMap = new HashMap<BenchmarkOperations, List<Double>>();
    List<Integer> nnCounts = new ArrayList<Integer>();
  }

  public static void combineHDFSandHopsFS(CompiledResults hdfsCr, CompiledResults hopsFsCr, String outputFolder) throws IOException {
    if (!hdfsCr.nnCounts.isEmpty() && hdfsCr.nnCounts.size() > 1) {
      System.out.println("Invalid Results for HDFS. HDFS can not have more than one NN");
      return;
    }

    if (hopsFsCr.nnCounts.isEmpty()) {
      System.err.println("No HopsFS Results Found");
      return;
    }
    
    if(hopsFsCr.nnCounts.get(0) != 1){
      System.err.print("The firt element in Hops Exeperimetn should be 1");
      return;
    }

    lines(hdfsCr, hopsFsCr, outputFolder);
    histogram(hdfsCr, hopsFsCr, outputFolder);

  }
  
  private static void histogram(CompiledResults hdfsCr, CompiledResults hopsFsCr, String outputFolder) throws IOException{
    
    String header = format("NameNodes:");
    header += format("SingleNN");
    for(Integer i : hopsFsCr.nnCounts){
      header+=format(i+"-NN");
    }
   
  
    boolean isFirstRecord = true;
    String col  = "t col";
    for (BenchmarkOperations op : hopsFsCr.valsMap.keySet()) {
      List<Double> hopsHisto = hopsFsCr.histoMap.get(op);
      String msg = format("#"+op)+"\n";
      msg+=header+"\n";
      msg+=format("HopsFS");
      msg+=format(RECORD_NOT_FOUND); // first col 
      for(Double val : hopsHisto){
        msg+=format(BenchmarkUtils.round(val)+"");
      }
      
      msg+="\n";
      msg+=format("HDFS");
      List<Double> hdfsVals = hdfsCr.valsMap.get(op);
      if (hdfsVals != null) {
        if (hdfsVals.size() > 1) {
          System.err.println("In Hdfs there should be only one value for " + op);
          System.exit(0);
        }

        if (hdfsVals.size() == 1) {
          msg += format(hdfsVals.get(0) + "");
        } else {
          msg += format(RECORD_NOT_FOUND);
        }
      }
      
      for (int i = 0; i < hopsFsCr.nnCounts.size(); i++) {
        msg += format(RECORD_NOT_FOUND);
      }
      msg+="\n";
      
      writeToFile(outputFolder+"/histogram-all-data.dat", msg, true);
      
      writeToFile(outputFolder+"/"+op+".dat", msg, false);
      
      String plotCommand = "";
      if(isFirstRecord){
        plotCommand += "plot ";
      }
      
      plotCommand += " newhistogram \""+op+"\", ";
      plotCommand += "\'"+op+".dat\' ";
      plotCommand += " using \"SingleNN\":xtic(1) not, "; 
      for(Integer i : hopsFsCr.nnCounts){
        plotCommand += "'' u \""+i+"-NN\" "+col+", ";
      }
      plotCommand +="\\\n";
      
      if(isFirstRecord){
        isFirstRecord = false;
        col = "not";
      }
      
      writeToFile(outputFolder+"/histo-internal.gnuplot", plotCommand, true);
        
      
    }
    
    
  }
  
  private static void lines(CompiledResults hdfsCr, CompiledResults hopsFsCr, String outputFolder) throws IOException{
    //  linear graph
    String outputFile = outputFolder+"/lines.txt";
    
    String header = format("NameNodes:");
    for(Integer i : hopsFsCr.nnCounts){
      header+=format(i+"-NN");
    }
    header+="\n";
    writeToFile(outputFile, header, true);

    
    for (BenchmarkOperations op : hopsFsCr.valsMap.keySet()) {
      List<Double> hopsVals = hopsFsCr.valsMap.get(op);
      String msg = format("HopsFS_" + op.toString());
      for (Double val : hopsVals) {
        msg += format(BenchmarkUtils.round(val) + "");
      }

      msg += "\n" + format("HDFS_" + op.toString());

      List<Double> hdfsVals = hdfsCr.valsMap.get(op);
      int hdfsRecordSize = 0;
      if (hdfsVals != null) {
        if (hdfsVals.size() > 1) {
          System.err.println("In Hdfs there should be only one value for " + op);
          System.exit(0);
        }

        if (hdfsVals.size() == 1) {
          msg += format(hdfsVals.get(0) + "");
        } else {
          msg += format(RECORD_NOT_FOUND);
        }
        hdfsRecordSize = 1;
      }
      
      for (int i = 0; i < hopsFsCr.nnCounts.size() - hdfsRecordSize; i++) {
        msg += format(RECORD_NOT_FOUND);
      }

      msg+="\n";
      writeToFile(outputFile, msg, true);
    }

  }

  private static void writeToFile(String file, String msg, boolean append) throws IOException {
    System.out.println(msg);
    FileWriter out = new FileWriter(file, append);
    out.write(msg);
    out.close();
  }
}
