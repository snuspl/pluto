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
package edu.snu.mist.core.task.groupAware.eventProcessors;

import edu.snu.mist.core.task.groupAware.Group;

/**
 * This is an interface of EventProcessor that processes events of queries.
 */
public interface EventProcessor extends AutoCloseable {

  /**
   * Start to execute the events of queries.
   */
  void start();

  /**
   * Get the load of the event processor.
   * @return load
   */
  double getLoad();

  /**
   * Set the load of the event processor.
   * @param l
   */
  void setLoad(double l);

  /**
   * Add the active groups to the active queue of the event processor.
   * @param group active group
   */
  void addActiveGroup(Group group);

  /**
   * Remove the active group from the active group queue.
   * @param group active group
   * @return true if there exists the active group in the queue
   */
  boolean removeActiveGroup(Group group);

  /**
   * Get the information of current runtime.
   */
  RuntimeProcessingInfo getCurrentRuntimeInfo();

  /**
   * Set whether it is running an isolated group.
   */
  void setRunningIsolatedGroup(boolean val);

  /**
   * Is running an isolated group or not.
   * @return true if it is running an isolated group.
   */
  boolean isRunningIsolatedGroup();
}