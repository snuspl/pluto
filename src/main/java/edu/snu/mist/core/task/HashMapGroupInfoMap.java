/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.core.task;

import javax.inject.Inject;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of GroupInfoMap that uses concurrent hash map.
 */
final class HashMapGroupInfoMap implements GroupInfoMap {

  private final ConcurrentHashMap<String, GroupInfo> map;

  @Inject
  private HashMapGroupInfoMap() {
    this.map = new ConcurrentHashMap<>();
  }

  @Override
  public GroupInfo get(final String conf) {
    return map.get(conf);
  }

  @Override
  public GroupInfo putIfAbsent(final String groupId, final GroupInfo groupInfo) {
    return map.putIfAbsent(groupId, groupInfo);
  }

  @Override
  public GroupInfo put(final String groupId, final GroupInfo groupInfo) {
    return map.put(groupId, groupInfo);
  }

  @Override
  public GroupInfo remove(final String groupId) {
    return map.remove(groupId);
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public Collection<GroupInfo> values() {
    return map.values();
  }
}
