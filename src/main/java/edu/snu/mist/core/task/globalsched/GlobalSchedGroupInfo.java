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

import edu.snu.mist.core.task.ExecutionDags;
import edu.snu.mist.core.task.OperatorChainManager;
import edu.snu.mist.core.task.QueryRemover;
import edu.snu.mist.core.task.QueryStarter;
import edu.snu.mist.core.task.deactivation.GroupSourceManager;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class which contains query and metric information about query group.
 * It is different from GroupInfo in that it does not have ThreadManager.
 * As we consider global scheduling, we do not have to have ThreadManger per group.
 */
@DefaultImplementation(DefaultGlobalSchedGroupInfo.class)
public interface GlobalSchedGroupInfo extends AutoCloseable {

  /**
   * Get the operator chain manager.
   * @return operator chain manager
   */
  OperatorChainManager getOperatorChainManager();

  /**
   * Add query id to the group.
   * @param queryId query id
   */
  void addQueryIdToGroup(String queryId);

  /**
   * Get the query starter.
   * @return query starter
   */
  QueryStarter getQueryStarter();

  /**
   * @return the list of query id in this group
   */
  List<String> getQueryIdList();

  /**
   * Return the execution dags in the group.
   * @return execution dags
   */
  ExecutionDags getExecutionDags();

  /**
   * Return the query remover.
   * @return query remover
   */
  QueryRemover getQueryRemover();

  /**
   * Get the number of remaining events.
   * @return the number of remaining events.
   */
  long numberOfRemainingEvents();

  /**
   * Get the load of the group.
   */
  double getLoad();

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
   * Get the GroupSourceManager.
   */
  GroupSourceManager getGroupSourceManager();

  /**
   * Get group id.
   * @return group id
   */
  String getGroupId();

  /**
   * Get the event processing time in the group.
   * @return event processing time
   */
  AtomicLong getProcessingTime();

  /**
   * Get the number of processed events in the group.
   * @return numb er of processed events
   */
  AtomicLong getProcessingEvent();

  /**
   * Set the group status to dispatched.
   * @return true if it changes from Ready -> Dispatched state
   */
  boolean setDispatched();

  /**
   * Set the group status to processing.
   * @return true if it changes from Dispatched -> Processing.
   */
  boolean setProcessing();

  /**
   * Set the group status to ready from processing status.
   * @return true if it changes from Processing -> Ready
   */
  boolean setReadyFromProcessing();

  /**
   * Set the group status to ready from Dispatched status.
   * @return true if it changes from Dispatched -> Ready
   */
  boolean setReadyFromDispatched();

  /**
   * Check whether it is in processing status.
   */
  boolean isProcessing();

  /**
   * Check whether it is in ready status.
   */
  boolean isReady();

  /**
   * Check whether it is in dispatched status.
   */
  boolean isDispatched();
}