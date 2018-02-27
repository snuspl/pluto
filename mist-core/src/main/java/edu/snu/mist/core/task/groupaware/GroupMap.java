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

import org.apache.reef.io.Tuple;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A map for managing MetaGroups.
 * The key is a groupId, and the value is the corresponding MetaGroup.
 */
public final class GroupMap {

  private final ConcurrentMap<String, Tuple<MetaGroup, AtomicBoolean>> map;

  @Inject
  private GroupMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public Tuple<MetaGroup, AtomicBoolean> get(final String groupId) {
    return map.get(groupId);
  }

  public Tuple<MetaGroup, AtomicBoolean> putIfAbsent(final String groupId,
                                                     final Tuple<MetaGroup, AtomicBoolean> groupInfo) {
    return map.putIfAbsent(groupId, groupInfo);
  }

  public Tuple<MetaGroup, AtomicBoolean> remove(final String groupId) {
    return map.remove(groupId);
  }

  public int size() {
    return map.size();
  }
}
