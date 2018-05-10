/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.core.driver;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The mapper class which contains application-jar information
 * which should be retrieved when the master fails.
 */
public final class ApplicationJarInfo {

  /**
   * The innermap which actually contains app-jar information.
   */
  private final ConcurrentMap<String, List<String>> innerMap;

  @Inject
  private ApplicationJarInfo() {
    this.innerMap = new ConcurrentHashMap<>();
  }

  public boolean put(final String appName,
                     final List<String> jarPaths) {
    if (innerMap.containsKey(appName)) {
      return false;
    } else {
      innerMap.put(appName, jarPaths);
      return true;
    }
  }

  public Set<Map.Entry<String, List<String>>> entrySet() {
    return innerMap.entrySet();
  }

}
