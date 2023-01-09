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

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author salman
 */
public interface FilePool {

  public String getDirToCreate();

  public String getFileToCreate();

  public void fileCreationSucceeded(String file);

  public String getFileToAppend();

  public String getFileToRead();

  public String getFileToStat();

  public String getDirToStat();

  public String getFileToInfo();

  public String getDirToInfo();

  public String getFileToSetReplication();

  public String getFilePathToChangePermissions();

  public String getDirPathToChangePermissions();

  public String getFileToRename();

  public void fileRenamed(String from, String to);

  public String getFileToDelete();

  public String getFileToChown();

  public String getDirToChown();

  public long getFileData(byte[] buffer) throws IOException;

  public long getNewFileSize() throws IOException;

  public boolean hasMoreFilesToWrite();
}
