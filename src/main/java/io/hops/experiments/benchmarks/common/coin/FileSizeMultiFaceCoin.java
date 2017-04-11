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
package io.hops.experiments.benchmarks.common.coin;

import io.hops.experiments.benchmarks.common.BenchmarkOperations;
import io.hops.experiments.utils.BenchmarkUtils;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;

/**
 *
 * @author salman
 */
public class FileSizeMultiFaceCoin {

  private class Point{
    long size;
    BigDecimal percentage;
    Point(long size, BigDecimal percentage){
      this.size = size;
      this.percentage = percentage;
    }
  }

  //private BigDecimal
  private Random rand;
  private BigDecimal expansion = new BigDecimal(100.00,new MathContext(4,RoundingMode.HALF_UP));
  //1000 face dice
  ArrayList<Long> dice = new ArrayList<Long>();

  public FileSizeMultiFaceCoin(String str) {
    this.rand = new Random(System.currentTimeMillis());
    createCoin(parse(str));
  }

  private void createCoin(List<Point> points) {

    BigDecimal total = new BigDecimal(0);
    for(Point point : points){
      total = total.add(point.percentage);
    }

    if (total.compareTo(new BigDecimal(100))!=0) {
      throw new IllegalArgumentException("All probabilities should add to 100. Got: " + total);
    }

    for(Point point : points){
      for (int i = 0; i < point.percentage.multiply(expansion).intValueExact(); i++) {
        dice.add(point.size);
      }
    }

    double expectedSize = expansion.multiply(new BigDecimal(100)).intValueExact();
    if (dice.size() != expectedSize) {
      Map<Long, Integer> counts = new HashMap<Long, Integer>();
      for (Long size : dice) {
        Integer opCount = counts.get(size);
        if (opCount == null) {
          opCount = new Integer(0);
        }
        opCount++;
        counts.put(size, opCount);
      }

      for (Long size : counts.keySet()) {
        double percent = ((double) counts.get(size) / ((double)expectedSize) * 100);
        System.out.println(size + " count " + counts.get(size) + ",  " + BenchmarkUtils.round(percent) + "%");
      }
      throw new IllegalStateException("Dice is not properfly created. Dice should have  " + expectedSize + " faces. Found " + dice.size());
    }

    Collections.shuffle(dice);
  }

  private List<Point> parse(String str){
    List<Point> points = new ArrayList<Point>();
    try{
      StringTokenizer strTok = new StringTokenizer(str,",[]()");
      while(strTok.hasMoreElements()){
        String size = strTok.nextToken();
        String percentage = strTok.nextToken();
        long s = Long.parseLong(size);
        double pd = Double.parseDouble(percentage);
        if(!BenchmarkUtils.isTwoDecimalPlace(pd)){
          throw new IllegalArgumentException("Wrong default Value. Only one decimal place is supported.");
        }
        points.add(new Point(s, new BigDecimal(pd)));
      }
    }catch (Exception e){
      throw new IllegalArgumentException("Malformed file size parameter. See documentation. Exception caused: "+e);
    }
    return points;
  }

  public Long getFileSize() {
    int choice = rand.nextInt(100 * expansion.intValueExact());
    return dice.get(choice);
  }

  public void testFlip() {
    Map<Long, Integer> counts = new HashMap<Long, Integer>();
    BigDecimal times = new BigDecimal(100000);
    for (int i = 0; i < times.intValueExact(); i++) {
      Long size = getFileSize();
      Integer opCount = counts.get(size);
      if (opCount == null) {
        opCount = new Integer(0);
      }
      opCount++;
      counts.put(size, opCount);
    }

    for (Long size : counts.keySet()) {
      double percent = (double) counts.get(size) / ( times.doubleValue()) * (double) 100;
      System.out.println(size + ": count: "+counts.get(size)+"        " + BenchmarkUtils.round(percent)+"%");
    }
  }
  
  public static void main(String [] argv){
    FileSizeMultiFaceCoin coin = new FileSizeMultiFaceCoin("[(1024,10),(2024,20),(3303,70)]");
    coin.testFlip();
  }
}
