/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
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

import io.hops.experiments.utils.DFSOperationsUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 *
 * @author salman
 */
public abstract class Aggregate {

  private DescriptiveStatistics speed = new DescriptiveStatistics();
  private DescriptiveStatistics sucessfulOps = new DescriptiveStatistics();
  private DescriptiveStatistics failedOps = new DescriptiveStatistics();
  private DescriptiveStatistics runDuration = new DescriptiveStatistics();

  public Aggregate() {
  }

  public void addSpeed(double val) {
    speed.addValue(val);
  }

  public void addSucessfulOps(double val) {
    sucessfulOps.addValue(val);
  }

  public void addFailedOps(double val) {
    failedOps.addValue(val);
  }

  public void addRunDuration(double val) {
    runDuration.addValue(val);
  }

  public double getSpeed() {
    return speed.getMean();
  }

  public double getMaxSpeed() {
    return speed.getMax();
  }

  public double getMinSpeed() {
    return speed.getMin();
  }

  public double getSucessfulOps() {
    return sucessfulOps.getMean();
  }

  public double getFailedOps() {
    return failedOps.getMean();
  }

  public double getRunDuration() {
    return runDuration.getMean();
  }
}