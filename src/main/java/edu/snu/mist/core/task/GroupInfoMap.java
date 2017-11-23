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

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Collection;

/**
 * This interface holds the group information.
 */
@DefaultImplementation(HashMapGroupInfoMap.class)
public interface GroupInfoMap {

  /**
   * Get the group info corresponding to the group id.
   * @param groupId the group id
   * @return group info of the group id
   */
  GroupInfo get(String groupId);

  /**
   * Put the group info to the map if it is absent.
   * @param groupId group id
   * @param groupInfo group info
   * @return the previous value associated with the group id,
   *         or {@code null} if there was no mapping for the group id
   */
  GroupInfo putIfAbsent(String groupId, GroupInfo groupInfo);

  /**
   * Put the group info to the map.
   * @param groupId the group id
   * @param groupInfo group info
   */
  GroupInfo put(String groupId, GroupInfo groupInfo);

  /**
   * Remove the group info.
   * @param groupId the group id
   */
  GroupInfo remove(String groupId);

  /**
   * Get the number of groups.
   * @return the number of groups
   */
  int size();

  /**
   * Get the collection of the GroupInfo.
   * @return collection of the group info.
   */
  Collection<GroupInfo> values();
}
