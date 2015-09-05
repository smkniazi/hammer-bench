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
package io.hops.experiments.coin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.utils.BenchmarkUtils;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author salman
 */
public class MultiFaceCoin {

  private BigDecimal create;
  private BigDecimal append;
  private BigDecimal read;
  private BigDecimal rename;
  private BigDecimal delete;
  private BigDecimal lsFile;
  private BigDecimal lsDir;
  private BigDecimal chmodFiles;
  private BigDecimal chmodDirs;
  private BigDecimal mkdirs;
  private BigDecimal setReplication;
  private BigDecimal fileInfo;
  private BigDecimal dirInfo;
  //private BigDecimal 
  private Random rand;
  private BigDecimal expansion = new BigDecimal(100.00,new MathContext(4,RoundingMode.HALF_UP));
  //1000 face dice
  ArrayList<BenchmarkOperations> dice = new ArrayList<BenchmarkOperations>();

  public MultiFaceCoin(BigDecimal create, BigDecimal append, BigDecimal read, BigDecimal rename, BigDecimal delete, BigDecimal lsFile,
          BigDecimal lsDir, BigDecimal chmodFiles, BigDecimal chmodDirs, BigDecimal mkdirs,
          BigDecimal setReplication, BigDecimal fileInfo, BigDecimal dirInfo) {
    this.create = create;
    this.append = append;
    this.read = read;
    this.rename = rename;
    this.delete = delete;
    this.chmodFiles = chmodFiles;
    this.chmodDirs = chmodDirs;
    this.mkdirs = mkdirs;
    this.lsFile = lsFile;
    this.lsDir = lsDir;
    this.setReplication = setReplication;
    this.fileInfo = fileInfo;
    this.dirInfo = dirInfo;

    this.rand = new Random(System.currentTimeMillis());

    createCoin();
  }

  private void createCoin() {

    System.out.println("Percentages create: " + create + " append: " + append + " read: " + read + " mkdir: "
            + mkdirs + " rename: " + rename + " delete: " + delete + " lsFile: "
            + lsFile + " lsDir: " + lsDir + " chmod files: " + chmodFiles + " chmod dirs: " + chmodDirs
            + " setReplication: " + setReplication + " fileInfo: " + fileInfo + " dirInfo: " + dirInfo);

    BigDecimal total = create.add(append).add(read).add(rename).add(delete).add(lsFile).add(lsDir)
            .add(chmodFiles).add(chmodDirs).add(mkdirs).add(setReplication).add(fileInfo).
            add(dirInfo);

    if (total.equals(new BigDecimal(100))) {
      throw new IllegalArgumentException("All probabilities should add to 100. Got: " + total);
    }

    
    for (int i = 0; i < create.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.CREATE_FILE);
    }

    for (int i = 0; i < append.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.APPEND_FILE);
    }

    for (int i = 0; i < read.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.READ_FILE);
    }

    for (int i = 0; i < rename.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.RENAME_FILE);
    }

    
    for (int i = 0; i < delete.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.DELETE_FILE);
    }

    for (int i = 0; i < lsFile.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.LS_FILE);
    }

    for (int i = 0; i < lsDir.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.LS_DIR);
    }

    for (int i = 0; i < chmodFiles.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.CHMOD_FILE);
    }

    for (int i = 0; i < chmodDirs.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.CHMOD_DIR);
    }

    for (int i = 0; i < mkdirs.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.MKDIRS);
    }

    for (int i = 0; i < setReplication.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.SET_REPLICATION);
    }

    for (int i = 0; i < fileInfo.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.FILE_INFO);
    }

    for (int i = 0; i < dirInfo.multiply(expansion).intValueExact(); i++) {
      dice.add(BenchmarkOperations.DIR_INFO);
    }

    double expectedSize = expansion.multiply(new BigDecimal(100)).intValueExact();
    if (dice.size() != expectedSize) {
      Map<BenchmarkOperations, Integer> counts = new HashMap<BenchmarkOperations, Integer>();
      for (BenchmarkOperations op : dice) {
        Integer opCount = counts.get(op);
        if (opCount == null) {
          opCount = new Integer(0);
        }
        opCount++;
        counts.put(op, opCount);
      }
      for (BenchmarkOperations op : counts.keySet()) {
        double percent = ((double) counts.get(op) / ((double)expectedSize) * 100);
                
        System.out.println(op + " count " + counts.get(op) + ",  " + BenchmarkUtils.round(percent) + "%");
      }
      throw new IllegalStateException("Dice is not properfly created. Dice should have  " + expectedSize + " faces. Found " + dice.size());
    }

    Collections.shuffle(dice);
  }

  public BenchmarkOperations flip() {
    int choice = rand.nextInt(100 * expansion.intValueExact());
    return dice.get(choice);
  }

  public void testFlip() {
    Map<BenchmarkOperations, Integer> counts = new HashMap<BenchmarkOperations, Integer>();
    BigDecimal times = new BigDecimal(100000);
    for (int i = 0; i < times.intValueExact(); i++) {
      BenchmarkOperations op = flip();
      Integer opCount = counts.get(op);
      if (opCount == null) {
        opCount = new Integer(0);
      }
      opCount++;
      counts.put(op, opCount);
    }


    for (BenchmarkOperations op : counts.keySet()) {
      double percent = (double) counts.get(op) / ( times.doubleValue()) * (double) 100;
      System.out.println(op + ": count: "+counts.get(op)+"        " + BenchmarkUtils.round(percent)+"%");
    }
  }
  
//  public static void main(String [] argv){
//    System.out.println(new BigDecimal((double)0.09,new MathContext(4,RoundingMode.HALF_UP)).multiply(new BigDecimal(100)).intValueExact());
//    
//    
//  }
}
