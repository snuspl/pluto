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
package edu.snu.mist.core.task.globalsched;

import edu.snu.mist.core.task.Query;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class which contains query and metric information about query group.
 * It is different from GroupInfo in that it does not have ThreadManager.
 * As we consider global scheduling, we do not have to have ThreadManger per group.
 */
@DefaultImplementation(DefaultSubGroupImpl.class)
public interface SubGroup extends AutoCloseable {

  void insert(Query query);

  boolean delete(Query query);

  /**
   * @return the list of query id in this group
   */
  List<Query> getQueryList();

  /**
   * Get the number of remaining events.
   * @return the number of remaining events.
   */
  long numberOfRemainingEvents();

  /**
   * Get the load of the group.
   */
  double getLoad();

  int processAllEvent();

  /**
   *  Set the load of the group.
   */
  void setLoad(double load);

  /**
   * Set the rebalance time.
   */
  void setLatestRebalanceTime(long rebalanceTime);

  /**
   * Get the latest rebalance time.
   * @return rebalance time
   */
  long getLatestRebalanceTime();

  /**
   * Check whether the group has events to be processed.
   * @return true if it has events to be processed.
   */
  boolean isActive();

  /**
   * Get sub-group id.
   * @return group id
   */
  String getSubGroupId();

  /**
   * Get the event processing time in the group.
   * @return event processing time
   */
  AtomicLong getProcessingTime();

  /**
   * Get the number of processed events in the group.
   * @return numb er of processed events
   * @return number of processed events
   */
  AtomicLong getProcessingEvent();

  Group getGroup();

  void setGroup(Group group);
}