/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.experiments.utils;


import io.hops.experiments.benchmarks.common.BenchMarkFileSystemName;
import io.hops.experiments.benchmarks.common.config.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;

public class MasterUtils {

  public static void setBaseDirMetaEnabled(Configuration config) throws
      IOException {
    if(config.getBenchMarkFileSystemName() == BenchMarkFileSystemName.HopsFS
        && config.isBaseDirMetaLogEnabled() && !config.getBaseDir().equals("/")){
      org.apache.hadoop.conf.Configuration dfsClientConf = new org.apache
          .hadoop.conf.Configuration();
      for(Object key : config.getFsConfig().keySet()){
        String keyStr = (String)key;
        String val = config.getFsConfig().getProperty(keyStr);
        dfsClientConf.set(keyStr, val);
      }
      DistributedFileSystem dfs = (DistributedFileSystem) DFSOperationsUtils
          .getDFSClient(dfsClientConf);

      dfs.mkdirs(new Path(config.getBaseDir()));
      dfs.setMetaEnabled(new Path(config.getBaseDir()), true);
      dfs.close();

    }
  }
}
