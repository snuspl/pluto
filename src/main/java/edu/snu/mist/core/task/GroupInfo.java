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
import java.util.ArrayList;
import java.util.List;

/**
 * A class which contains query and metric information about query group.
 */
public final class GroupInfo {

  /**
   * List of query ids belonging to this GroupInfo.
   */
  private final List<String> queryIdList;

  /**
   * The number of all events inside the group operator chain queues.
   */
  private long numEvents;

  @Inject
  private GroupInfo() {
    queryIdList = new ArrayList<>();
    numEvents = 0;
  }

  public void addQueryIdToGroup(final String queryId) {
    queryIdList.add(queryId);
  }

  public List<String> getQueryIdList() {
    return queryIdList;
  }

  public void setNumEvents(final long numEventsParam) {
    this.numEvents = numEventsParam;
  }

  public long getNumEvents() {
    return numEvents;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof GroupInfo)) {
      return false;
    }
    final GroupInfo groupInfo = (GroupInfo) o;
    return this.queryIdList.equals(groupInfo.queryIdList) &&
        this.numEvents == groupInfo.numEvents;
  }

  @Override
  public int hashCode() {
    return this.queryIdList.hashCode() * 31;
  }
}