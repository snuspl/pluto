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

import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.task.ExecutionDags;
import edu.snu.mist.core.task.OperatorChainManager;
import edu.snu.mist.core.task.QueryRemover;
import edu.snu.mist.core.task.QueryStarter;
import edu.snu.mist.core.task.metrics.GroupMetrics;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * This is the default implementation of the GlobalSchedGroupInfo.
 */
final class DefaultGlobalSchedGroupInfo implements GlobalSchedGroupInfo {

  /**
   * Group id.
   */
  private final String groupId;

  /**
   * List of query ids belonging to this GroupInfo.
   */
  private final List<String> queryIdList;

  /**
   * Execution dags that are currently running in this group.
   */
  private final ExecutionDags executionDags;

  /**
   * A query starter.
   */
  private final QueryStarter queryStarter;

  /**
   * Operator chain manager.
   */
  private final OperatorChainManager operatorChainManager;

  /**
   * Query remover that deletes queries.
   */
  private final QueryRemover queryRemover;

  /**
   * The latest scheduled time of the group.
   */
  private long latestScheduledTime;

  /**
   * The vruntime of the group.
   */
  private double vruntime;

  /**
   * A metric holder that contains weight and the number of events in the group, which will be updated periodically.
   */
  private final GroupMetrics metricHolder;

  @Inject
  private DefaultGlobalSchedGroupInfo(@Parameter(GroupId.class) final String groupId,
                                      final ExecutionDags executionDags,
                                      final QueryStarter queryStarter,
                                      final OperatorChainManager operatorChainManager,
                                      final QueryRemover queryRemover,
                                      final GroupMetrics metricHolder) {
    this.groupId = groupId;
    this.queryIdList = new ArrayList<>();
    this.executionDags = executionDags;
    this.queryStarter = queryStarter;
    this.operatorChainManager = operatorChainManager;
    this.queryRemover = queryRemover;
    this.latestScheduledTime = 0;
    this.vruntime = 0;
    this.metricHolder = metricHolder;
  }

  /**
   * Get the operator chain manager.
   * @return operator chain manager
   */
  @Override
  public OperatorChainManager getOperatorChainManager() {
    return operatorChainManager;
  }

  /**
   * Add a query id into this group.
   * @param queryId the query id to add
   */
  @Override
  public void addQueryIdToGroup(final String queryId) {
    queryIdList.add(queryId);
  }

  /**
   * Get the query starter.
   * @return query starter
   */
  @Override
  public QueryStarter getQueryStarter() {
    return queryStarter;
  }

  /**
   * @return the list of query id in this group
   */
  @Override
  public List<String> getQueryIdList() {
    return queryIdList;
  }

  /**
   * Return the execution dags in the group.
   * @return execution dags
   */
  @Override
  public ExecutionDags getExecutionDags() {
    return executionDags;
  }

  /**
   * Return the query remover.
   * @return query remover
   */
  @Override
  public QueryRemover getQueryRemover() {
    return queryRemover;
  }

  @Override
  public long getLatestScheduledTime() {
    return latestScheduledTime;
  }

  @Override
  public void setLatestScheduledTime(final long time) {
    latestScheduledTime = time;
  }

  @Override
  public GroupMetrics getMetricHolder() {
    return metricHolder;
  }

  @Override
  public double getVRuntime() {
    return vruntime;
  }

  @Override
  public void setVRuntime(final double vrt) {
    vruntime = vrt;
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public String toString() {
    return groupId;
  }
}