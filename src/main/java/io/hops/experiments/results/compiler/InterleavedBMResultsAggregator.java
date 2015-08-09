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

import io.hops.experiments.results.InterleavedBMResults;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author salman
 */
public class InterleavedBMResultsAggregator {

  Map<Integer/*NN Count*/, InterleavedAggregate/*aggregates*/> allResults =
          new HashMap<Integer, InterleavedAggregate>();

  public void processRecord(InterleavedBMResults result) {
    //System.out.println(result);

    InterleavedAggregate agg = allResults.get(result.getNoOfNamenodes());

    if (agg == null) {
      agg = new InterleavedAggregate();
      allResults.put(result.getNoOfNamenodes(), agg);
    }

    agg.addSpeed(result.getSpeed());
    agg.addFailedOps(result.getFailedOps());
    agg.addSucessfulOps(result.getSuccessfulOps());
    agg.addRunDuration(result.getDuration());
  }

  void processAllRecords() {
    System.out.println("Generating compiled results for INTERLEAVED Benchmarks");
    for (Integer key : allResults.keySet()) {
      InterleavedAggregate agg = allResults.get(key);
      System.out.println("NN Count " + key + " Speed " + agg.getSpeed());
    }
  }
}
