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
package edu.snu.mist.core.task.groupaware;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A map for managing application meta info.
 * The key is a app id, and the value is the corresponding application info.
 */
public final class ApplicationMap {

  private final ConcurrentMap<String, ApplicationInfo> map;

  @Inject
  private ApplicationMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public ApplicationInfo get(final String appId) {
    return map.get(appId);
  }

  public ApplicationInfo putIfAbsent(final String groupId,
                                         final ApplicationInfo applicationInfo) {
    return map.putIfAbsent(groupId, applicationInfo);
  }

  public ApplicationInfo remove(final String groupId) {
    return map.remove(groupId);
  }

  public int size() {
    return map.size();
  }
}
