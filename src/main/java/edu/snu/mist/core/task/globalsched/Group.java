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

import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * A class which contains query and metric information about query group.
 * It is different from GroupInfo in that it does not have ThreadManager.
 * As we consider global scheduling, we do not have to have ThreadManger per group.
 */
@DefaultImplementation(DefaultGroupImpl.class)
public interface Group extends AutoCloseable {

  void addSubGroup(SubGroup subGroup);

  List<SubGroup> getSubGroups();

  /**
   * Add query to the group.
   */
  void insert(SubGroup subGroup);

  void delete(SubGroup subGroup);

  void setEventProcessor(EventProcessor eventProcessor);

  EventProcessor getEventProcessor();

  /**
   * Get the load of the group.
   */
  double getLoad();

  String getGroupId();

  /**
   *  Set the load of the group.
   */
  void setLoad(double load);

  /**
   * Check whether the group has events to be processed.
   * @return true if it has events to be processed.
   */
  boolean isActive();

  double calculateLoad();

  int processAllEvent();
}