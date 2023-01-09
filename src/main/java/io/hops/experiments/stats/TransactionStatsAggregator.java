/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.experiments.stats;

import com.google.common.collect.Maps;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TransactionStatsAggregator {

  public final static String ALL = "*";
  private final File statsCSVFile;
  private final String transaction;
  private final String headerPattern;

  public TransactionStatsAggregator(File statsCSVFile,
                                    String transaction, String headerPattern) {
    this.statsCSVFile = statsCSVFile;
    this.transaction = transaction.toUpperCase();
    this.headerPattern = headerPattern.toUpperCase();
  }

  public void aggregate() throws IOException {
    aggregate(statsCSVFile, headerPattern, transaction, true);
  }


  public static Map<String, DescriptiveStatistics> aggregate(File statsFile,
                                                             String transaction) throws IOException {
    return aggregate(statsFile, ALL, transaction, false);
  }

  public static Map<String, DescriptiveStatistics> aggregate(File statsFile,
                                                             String header, String transaction) throws IOException {
    return aggregate(statsFile, header, transaction, false);
  }

  public static Map<String, DescriptiveStatistics> aggregate(File statsFile,
                                                             String headerPattern, String transaction, boolean printSummary)
          throws IOException {
    if (!statsFile.exists())
      return null;

    transaction = transaction.toUpperCase();

    BufferedReader reader = new BufferedReader(new FileReader(statsFile));
    String tx = reader.readLine();
    String[] headers = null;
    Map<Integer, DescriptiveStatistics> statistics = Maps.newHashMap();
    if (tx != null) {
      headers = tx.split(",");
      for (int i = 1; i < headers.length; i++) {
        String h = headers[i].toUpperCase();
        if (h.contains(headerPattern) || headerPattern.equals(ALL)) {
          statistics.put(i, new DescriptiveStatistics());
        }
      }
    }

    int txCount = 0;
    while ((tx = reader.readLine()) != null) {
      if (tx.startsWith(transaction) || transaction.equals(ALL)) {
        txCount++;
        String[] txStats = tx.split(",");
        if (txStats.length == headers.length) {
          for (Map.Entry<Integer, DescriptiveStatistics> e : statistics
                  .entrySet()) {
            e.getValue().addValue(Double.valueOf(txStats[e.getKey()]));
          }
        }
      }
    }

    reader.close();

    if (headers == null)
      return null;

    if (printSummary) {
      System.out.println("Transaction: " + transaction + " " + txCount);

      List<Integer> keys = new ArrayList<Integer>(statistics.keySet());
      Collections.sort(keys);

      for (Integer i : keys) {
        DescriptiveStatistics stats = statistics.get(i);
        if (stats.getMin() == 0 && stats.getMax() == 0) {
          continue;
        }
        System.out.println(headers[i]);
        System.out.println("Min " + stats.getMin() + " Max " + stats.getMax() +
                " Avg " + stats.getMean() + " Std " + stats.getStandardDeviation());
      }
    }

    Map<String, DescriptiveStatistics> annotatedStats = Maps
            .newHashMap();
    for (Map.Entry<Integer, DescriptiveStatistics> e : statistics
            .entrySet()) {
      annotatedStats.put(headers[e.getKey()].trim(), e.getValue());
    }
    return annotatedStats;
  }

}
