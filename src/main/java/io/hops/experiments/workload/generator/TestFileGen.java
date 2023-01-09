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
package io.hops.experiments.workload.generator;


import io.hops.experiments.benchmarks.common.config.ConfigKeys;

/**
 *
 * @author salman
 */
public class TestFileGen {
  public static void main(String[] argv) {
    FixeDepthFileTreeGenerator gen = new FixeDepthFileTreeGenerator("/test", 5, ConfigKeys.FILE_SIZE_IN_Bytes_DEFAULT);

    for (int i = 0; i < 10000; i++) {
      System.out.println(gen.getDirToCreate());
    }


    for (int i = 0; i < 10; i++) {
      System.out.println(gen.getFileToCreate());
    }
  }

}
