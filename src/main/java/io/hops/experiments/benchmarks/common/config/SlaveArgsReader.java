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
package io.hops.experiments.benchmarks.common.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * @author salman
 */
public class SlaveArgsReader {

  private int slaveListeningPort = 0;

  public SlaveArgsReader(String configFilePath) throws FileNotFoundException, IOException {
    loadPropFile(configFilePath);
  }

  private Properties loadPropFile(String configFilePath) throws FileNotFoundException, IOException {
    final String PROP_FILE = configFilePath;
    Properties props = new Properties();
    InputStream input = new FileInputStream(PROP_FILE);
    props.load(input);
    slaveListeningPort = Integer.parseInt(props.getProperty("slave.listening.port"));
    return props;
  }

  public int getSlaveListeningPort() {
    return slaveListeningPort;
  }
}
