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

import com.google.common.base.Joiner;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

public class TransactionStatsCumulative {

  /*
   threads_n
      depth_d
        mem
        nomem
   */
  private static final String SEPERATOR = "_";
  private static final String CLIENT = "threads";
  private static final String DEPTH = "depth";
  private static final String MEMSTATS = "memcache-stats";
  private static final String HOPSSTATS = "hops-stats.csv";
  private static final String RESOLVING_CACHE_STATS = "hops-resolving-cache-stats.csv";

  private static final String BASE = "Base";
  private static final String INMEMORY = "InMemory";
  private static final String INODE_MEMCACHE = "INode";
  private static final String PATH_MEMCACHE = "Path";

  private static final String[] RESOLVING_CACHES = {BASE, INMEMORY,
          INODE_MEMCACHE, PATH_MEMCACHE};

  private static final String[] RESOLVING_CACHE_COLUMNS = new
          String[]{"Elapsed",
          "RoundTrips"};

  private static final Integer[] CLIENTS = new Integer[]{10, 25, 50, 75, 100,
          125, 150};
  private static final Integer[] DEPTHS = new Integer[]{5, 10, 15, 20, 25};

  private final File statsDir;

  public TransactionStatsCumulative(File statsDir) {
    this.statsDir = statsDir;
  }

  public void dump(String transaction, String[] columns) throws IOException {
    dumpClients(transaction, columns);
    dumpDepths(transaction, columns);
  }

  public void dumpClients(String transaction, String[] columns) throws IOException {
    if (statsDir.exists()) {

      File clientsStats = new File(statsDir, transaction +
              "-cumlativestats-clients");
      clientsStats.mkdirs();

      for (int client : CLIENTS) {
        File clientDir = new File(statsDir, CLIENT + SEPERATOR + client);
        if (clientDir.exists()) {

          File clientStats = new File(clientsStats, String.valueOf(client));
          BufferedWriter writer = new BufferedWriter(new FileWriter(clientStats));

          writer.write("# Depth " + Joiner.on(" ").join(columns) + " " +
                  Joiner.on(" ").join(RESOLVING_CACHE_COLUMNS));
          writer.newLine();

          for (int depth : DEPTHS) {

            File depthDir = new File(clientDir, DEPTH + SEPERATOR + depth);
            if (depthDir.exists()) {
              System.out.println(client + " clients - Depth " + depth);
              writeStats(writer, transaction, columns, depthDir, depth);
            }
          }
          writer.close();
        }
      }
      generateGraphsFor(transaction, clientsStats, columns, "TotalTime",
              "File Tree Depth", "Time (miliseconds)", "Clients");
    }
  }


  public void dumpDepths(String transaction, String[] columns) throws
          IOException {
    if (statsDir.exists()) {

      File depthsStats = new File(statsDir, transaction +
              "-cumlativestats-depths");
      depthsStats.mkdirs();
      for (int depth : DEPTHS) {
        File depthStats = new File(depthsStats, String.valueOf(depth));
        BufferedWriter writer = new BufferedWriter(new FileWriter(depthStats));

        writer.write("# Clients " + Joiner.on(" ").join(columns) + " " +
                Joiner.on(" ").join(RESOLVING_CACHE_COLUMNS));
        writer.newLine();

        for (int client : CLIENTS) {
          File clientDir = new File(statsDir, CLIENT + SEPERATOR + client);
          if (clientDir.exists()) {
            File depthDir = new File(clientDir, DEPTH + SEPERATOR + depth);
            if (depthDir.exists()) {
              System.out.println(client + " clients - Depth " + depth);
              writeStats(writer, transaction, columns, depthDir, client);
            }
          }
        }

        writer.close();
      }

      generateGraphsFor(transaction, depthsStats, columns, "TotalTime",
              "Clients", "Time (miliseconds)", DEPTH);
    }
  }

  private void writeStats(BufferedWriter writer, String transaction, String[]
          columns, File depthDir, int key) throws IOException {

    writer.write(key + " ");

    for (String cache : RESOLVING_CACHES) {
      File cacheDir = new File(depthDir, cache);
      Map<String, DescriptiveStatistics> stats = null;
      Map<String, DescriptiveStatistics> statsResolving = null;
      if (cacheDir.exists()) {
        stats = TransactionStatsAggregator.aggregate(new File(cacheDir,
                HOPSSTATS), transaction);
      }

      if (isResolvingCache(cache)) {
        statsResolving = TransactionStatsAggregator.aggregate(new File
                (cacheDir, RESOLVING_CACHE_STATS), "GET");
      }

      if (stats != null) {
        for (String col : columns) {
          DescriptiveStatistics st = stats.get(col);
          writer.write(st.getMin() + " " + st.getMean() + " " +
                  st.getMax() + " ");
        }
      }

      if (statsResolving != null) {
        for (String col : RESOLVING_CACHE_COLUMNS) {
          DescriptiveStatistics st = statsResolving.get(col);
          writer.write(st.getMin() + " " + st.getMean() + " " +
                  st.getMax() + " ");
        }
      }
    }

    writer.newLine();
  }

  private boolean isResolvingCache(String cache) {
    return !cache.equals(BASE);
  }

  private String getResolvingCacheTitle(String cache) {
    if (cache.equals(INODE_MEMCACHE) || cache.equals(PATH_MEMCACHE)) {
      return cache + "Memcache";
    }
    return cache;
  }

  private void generateGraphsFor(String transaction, File baseDir, String[]
          columns, String graphCol, String xlabel, String ylabel, String title)
          throws IOException {
    String[] datFiles = baseDir.list();
    int i = 0;
    for (String col : columns) {
      if (col.equals(graphCol)) {
        break;
      }
      i++;
    }

    Arrays.sort(datFiles, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return Integer.valueOf(o1).compareTo(Integer.valueOf(o2));
      }
    });

    int baseOffset = 1;
    int[] lines = new int[RESOLVING_CACHES.length];
    String[] linesTitles = new String[RESOLVING_CACHES.length];

    for (int c = 0; c < RESOLVING_CACHES.length; c++) {
      lines[c] = baseOffset + i * 3 + 2;
      linesTitles[c] = getResolvingCacheTitle(RESOLVING_CACHES[c]);
      baseOffset += (columns.length + (isResolvingCache(RESOLVING_CACHES[c]) ?
              RESOLVING_CACHE_COLUMNS.length : 0)) * 3;
    }

    for (String datFile : datFiles) {
      String name = transaction.replace("_", "-") + "-" + datFile + "-" + title;
      String graph = getGnuScript(name, name, xlabel, ylabel,
              datFile, lines, linesTitles);
      FileWriter writer = new FileWriter(new File(baseDir, name + ".gnu"));
      writer.write(graph);
      writer.close();
    }


    for (int rccol = 0; rccol < RESOLVING_CACHE_COLUMNS.length; rccol++) {

      baseOffset = 1 + columns.length * 3;

      for (int rc = 1; rc < RESOLVING_CACHES.length; rc++) {
        String name = transaction.replace("_", "-") + "-" +
                RESOLVING_CACHE_COLUMNS[rccol] + "-" + getResolvingCacheTitle
                (RESOLVING_CACHES[rc]);

        int line = baseOffset + (columns.length + rccol) * 3 + 2;

        String graph = getGnuScript(name, name, xlabel, rccol == 1 ?
                "RoundTrips" : ylabel, datFiles, line);

        FileWriter writer = new FileWriter(new File(baseDir, name + ".gnu"));
        writer.write(graph);
        writer.close();

        baseOffset += (columns.length + RESOLVING_CACHE_COLUMNS.length) * 3;
      }
    }
  }

  private String getGnuScript(String fileName, String title, String xlabel,
                              String ylabel, String[] datFiles, int line) {
    return getGnuScript(fileName, title, xlabel, ylabel, datFiles,
            new int[]{line}, null);
  }

  private String getGnuScript(String fileName, String title, String xlabel,
                              String ylabel, String datfile, int lines[], String[] linesTitle) {
    return getGnuScript(fileName, title, xlabel, ylabel, new String[]{datfile},
            lines, linesTitle);
  }

  private String getGnuScript(String fileName, String title, String xlabel,
                              String ylabel, String[] datFiles, int lines[], String[] linesTitle) {
    String graph = "set terminal png enhanced #postscript eps enhanced color " +
            "\n";
    graph += "set style data lines \n";
    graph += "set border 3 \n";
    graph += "set key outside \n";
    graph += "set grid \n";
    graph += "set title \"" + title + "\" \n";
    graph += "set output '" + fileName + ".png' \n";
    graph += "set xlabel \"" + xlabel + "\" \n";
    graph += "set ylabel \"" + ylabel + "\" \n";

    graph += "plot ";

    for (String datfile : datFiles) {
      for (int i = 0; i < lines.length; i++) {
        graph += "\"" + datfile + "\" using " + lines[i] + ":xtic(1) title";
        if (linesTitle != null) {
          graph += "\"" + linesTitle[i] + "\", ";
        } else {
          graph += "\"" + datfile + "\", ";
        }
      }
    }
    return graph.substring(0, graph.length() - 2);
  }

  public static void main(String[] args) throws IOException {
    TransactionStatsCumulative txs = new TransactionStatsCumulative(new File
            ("/home/maism/src/hops/stats_01"));
    txs.dump("start_file", new String[]{"TotalTime"});

    txs.dump("get_block_locations", new String[]{"TotalTime"});

    txs.dump("complete_file", new String[]{"TotalTime"});
  }
}
