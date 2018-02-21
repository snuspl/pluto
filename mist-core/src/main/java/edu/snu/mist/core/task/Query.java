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
package edu.snu.mist.core.task;

import edu.snu.mist.core.task.groupaware.Group;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This interface represents a query in MistTask.
 */
@DefaultImplementation(DefaultQueryImpl.class)
public interface Query {

  /**
   * Set the group of the query.
   * This can change when performing group splitting.
   * @param group group
   */
  void setGroup(Group group);

  /**
   * Insert an active source.
   */
  void insert(SourceOutputEmitter sourceOutputEmitter);

  /**
   * Delete an active source.
   */
  void delete(SourceOutputEmitter sourceOutputEmitter);

  /**
   * Process all the events in the query.
   * @return number of processed events
   */
  int processAllEvent();

  /**
   * Get the query id.
   * @return query id
   */
  String getId();

  /**
   * Get the group that contains this query.
   * @return group
   */
  Group getGroup();

  /**
   * Get the number of processed events.
   * @return processed events
   */
  AtomicLong getProcessingEvent();

  /**
   * Set the query load.
   */
  void setLoad(double load);

  /**
   * Get the query load.
   * @return
   */
  double getLoad();

  /**
   * The number of remaining events.
   */
  long numberOfRemainingEvents();

  /**
   * Change the query status to ready.
   */
  void setReady();

  /**
   * Change the query status from processing to ready.
   * @return true if the status sets to ready.
   */
  boolean setProcessingFromReady();
}