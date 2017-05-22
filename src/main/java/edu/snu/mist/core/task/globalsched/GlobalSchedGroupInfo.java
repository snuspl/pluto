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

import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.deactivation.GroupSourceManager;
import edu.snu.mist.core.task.metrics.GroupMetrics;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

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
   * Get the latest scheduled time of the group.
   * @return the latest scheduled time
   */
  long getLatestScheduledTime();

  /**
   * Set the latest scheduled time of the group.
   * @param time the latest scheduled time
   */
  void setLatestScheduledTime(long time);

  /**
   * Get the metric holder contains the number of events and weight metric of this group.
   * @return the metric holder of this group
   */
  GroupMetrics getMetricHolder();

  /**
   * Get the vruntime of the group.
   * @return vruntime
   */
  double getVRuntime();

  /**
   * Set the vruntime of the group.
   * @param vruntime vruntime
   */
  void setVRuntime(double vruntime);

  /**
   * Get the GroupSourceManager.
   */
  GroupSourceManager getGroupSourceManager();
}