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
 * A map for managing MetaGroups.
 * The key is a app id, and the value is the corresponding MetaGroup.
 */
public final class GroupMap {

  private final ConcurrentMap<String, MetaGroup> map;

  @Inject
  private GroupMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public MetaGroup get(final String groupId) {
    return map.get(groupId);
  }

  public MetaGroup putIfAbsent(final String groupId,
                               final MetaGroup metaGroup) {
    return map.putIfAbsent(groupId, metaGroup);
  }

  public MetaGroup remove(final String groupId) {
    return map.remove(groupId);
  }

  public int size() {
    return map.size();
  }
}
