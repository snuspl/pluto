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

import edu.snu.mist.core.task.Query;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class which contains query and metric information about query group.
 * It is different from GroupInfo in that it does not have ThreadManager.
 * As we consider global scheduling, we do not have to have ThreadManger per group.
 */
@DefaultImplementation(DefaultGroupImpl.class)
public interface Group extends AutoCloseable {

  /**
   * Add a query assigned to the group.
   */
  void addQuery(Query query);

  /**
   * Get the list of queries assigned to the group.
   */
  List<Query> getQueries();

  /**
   * Add an active query to the group.
   */
  void insert(Query query);

  /**
   * Delete an active query from the group.
   */
  void delete(Query query);

  /**
   * Set an event processor. This can be changed when group is reassigned.
   */
  void setEventProcessor(EventProcessor eventProcessor);

  /**
   * Get an event processor that processes the events of the group.
   */
  EventProcessor getEventProcessor();

  /**
   * Get a meta group.
   */
  MetaGroup getMetaGroup();

  /**
   * Set a meta group.
   */
  void setMetaGroup(MetaGroup metaGroup);

  /**
   * Change the group status from ready to processing.
   * @return true if it is changed from ready to processing.
   */
  boolean setProcessingFromReady();

  /**
   * Set the group status to ready.
   */
  void setReady();

  /**
   * Get the load of the group.
   */
  double getLoad();

  /**
   * Get the group id.
   */
  String getGroupId();

  /**
   * Get processing time.
   */
  AtomicLong getProcessingTime();

  /**
   * Get the number of processed events.
   */
  AtomicLong getProcessingEvent();

  /**
   *  Set the load of the group.
   */
  void setLoad(double load);

  /**
   * Check whether the group has events to be processed.
   * @return true if it has events to be processed.
   */
  boolean isActive();

  /**
   * Process all events in the group.
   * @return the number of processed events
   */
  int processAllEvent();

  /**
   * Process all events in the group within the timeout.
   * @return the number of processed events
   */
  int processAllEvent(long timeout);

  /**
   * Is this group split?
   */
  boolean isSplited();

  /**
   * Get the latest time that the group is assigned to another event processor.
   */
  long getLatestMovedTime();

  /**
   * Set the latest time that the group is assigned to another event processor.
   */
  void setLatestMovedTime(long t);

  /**
   * The number of remaining events.
   * @return
   */
  long numberOfRemainingEvents();

  /**
   * The number of active queries.
   */
  int size();
}