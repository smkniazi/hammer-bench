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
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

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
          if(diff < 0){
            diff = 0;
          }
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
  
    String header = format("NameNodes");
    header += format("SingleNN");
    for(Integer i : hopsFsCr.nnCounts){
      header+=format(i+"-NN");
    }
   
  
    boolean isFirstRecord = true;
    String col  = "t col";
    
    //TODO use String buffer everywhere
    String allData = "";
    String plotCommands = "";
    
    SortedSet<BenchmarkOperations> sorted = new TreeSet<BenchmarkOperations>();
    sorted.addAll(hopsFsCr.valsMap.keySet());
    for (BenchmarkOperations op : sorted) {
      int colorIndex = 0;
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
      
      allData += msg;
      
      writeToFile(outputFolder+"/"+op+".dat", msg, false);
      
      String plotCommand = "";
      if(isFirstRecord){
        plotCommand += "plot ";
      }
      
      plotCommand += " newhistogram \""+op.toString().replace("_", "\\n")+"\", ";
      plotCommand += "\'"+op+".dat\' ";
      plotCommand += " using \"SingleNN\":xtic(1) not "+getColor(colorIndex++)+", "; 
      for(Integer i : hopsFsCr.nnCounts){
        plotCommand += "'' u \""+i+"-NN\" "+col+getColor(colorIndex++)+" , ";
      }
      plotCommand +="\\\n";
      
      if(isFirstRecord){
        isFirstRecord = false;
        col = "not";
      }
      

      plotCommands+=plotCommand;
        
      
    }
    writeToFile(outputFolder+"/histo-internal.gnuplot", plotCommands, false);
    writeToFile(outputFolder+"/histogram-all-data.dat", allData, false);
  }
  
  private static String getColor(int index){
    //String[] colorMap = {"#8dd3c7", "#ffffb3", "#bebada", "#fb8072", "#80b1d3", "#fdb462", "#b3de69", "#fccde5", "#d9d9d9" };
    String[] colorMap = {"#a6cee3", "#1f78b4", "#b2df8a", "#33a02c" , "#fb9a99", "#e31a1c", "#fdbf6f" , "#ff7f00", "#cab2d6", "#6a3d9a", "#ffff99", "#b15928" };
    return " lc rgb '"+ colorMap[index % colorMap.length]+"' ";
  }
  
  
  
  private static void lines(CompiledResults hdfsCr, CompiledResults hopsFsCr, String outputFolder) throws IOException{
    //  linear graph
    String outputFile = outputFolder+"/lines.txt";
    String allData = "";
    String header = format("NameNodes:");
    for(Integer i : hopsFsCr.nnCounts){
      header+=format(i+"-NN");
    }
    header+="\n";
    allData+=header;

    
    SortedSet<BenchmarkOperations> sorted = new TreeSet<BenchmarkOperations>();
    sorted.addAll(hopsFsCr.valsMap.keySet());
    for (BenchmarkOperations op : sorted) {
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
      allData+=msg;
    }
    
    writeToFile(outputFile, allData, false);

  }

  private static void writeToFile(String file, String msg, boolean append) throws IOException {
    System.out.println(msg);
    FileWriter out = new FileWriter(file, append);
    out.write(msg);
    out.close();
  }
}
