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

import java.util.ArrayList;

/**
 *
 * @author salman
 */
public class PathUtils {
  public static final String SEPARATOR = "/";
  public static final char SEPARATOR_CHAR = '/';

  public static String[] getPathNames(String path) {
    if (path == null || !path.startsWith(SEPARATOR)) {
      throw new AssertionError("Absolute path required");
    }
    return split(path, SEPARATOR_CHAR);
  }

  public static String[] split(
          String str, char separator) {
    // String.split returns a single empty result for splitting the empty
    // string.
    if (str.isEmpty()) {
      return new String[]{""};
    }
    ArrayList<String> strList = new ArrayList<String>();
    int startIndex = 0;
    int nextIndex = 0;
    while ((nextIndex = str.indexOf(separator, startIndex)) != -1) {
      strList.add(str.substring(startIndex, nextIndex));
      startIndex = nextIndex + 1;
    }
    strList.add(str.substring(startIndex));
    // remove trailing empty split(s)
    int last = strList.size(); // last split
    while (--last >= 0 && "".equals(strList.get(last))) {
      strList.remove(last);
    }
    return strList.toArray(new String[strList.size()]);
  }
}
