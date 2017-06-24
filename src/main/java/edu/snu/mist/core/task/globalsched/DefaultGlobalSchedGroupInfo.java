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
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.deactivation.GroupSourceManager;
import edu.snu.mist.core.task.globalsched.parameters.DefaultGroupLoad;
import edu.snu.mist.core.task.metrics.EWMAMetric;
import edu.snu.mist.core.task.metrics.GroupMetrics;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
   * The latest inactive time of the group.
   */
  private long latestInactiveTime;

  /**
   * The load of the group.
   */
  private final EWMAMetric load;

  /**
   * A metric holder that contains weight and the number of events in the group, which will be updated periodically.
   */
  private final GroupMetrics metricHolder;

  /**
   * Assigned value whether this group is assigned to an event processor or not.
   */
  private final AtomicBoolean assigned = new AtomicBoolean(false);

  /**
   * GroupSourceManager for this group.
   */
  private final GroupSourceManager groupSourceManager;

  /**
   * Default load of the group.
   */
  private final double defaultLoad;

  @Inject
  private DefaultGlobalSchedGroupInfo(@Parameter(GroupId.class) final String groupId,
                                      @Parameter(DefaultGroupLoad.class) final double defaultLoad,
                                      final ExecutionDags executionDags,
                                      final QueryStarter queryStarter,
                                      final OperatorChainManager operatorChainManager,
                                      final QueryRemover queryRemover,
                                      final GroupMetrics metricHolder,
                                      final GroupSourceManager groupSourceManager) {
    this.groupId = groupId;
    this.defaultLoad = defaultLoad;
    this.queryIdList = new ArrayList<>();
    this.executionDags = executionDags;
    this.queryStarter = queryStarter;
    this.operatorChainManager = operatorChainManager;
    this.queryRemover = queryRemover;
    this.latestInactiveTime = System.currentTimeMillis();
    this.load = new EWMAMetric(defaultLoad, 0.7);
    this.metricHolder = metricHolder;
    this.groupSourceManager = groupSourceManager;
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
  public void setLatestInactiveTime(final long time) {
    latestInactiveTime = time;
  }

  @Override
  public long getLatestInactiveTime() {
    return latestInactiveTime;
  }

  @Override
  public long numberOfRemainingEvents() {
    return operatorChainManager.numEvents();
  }

  @Override
  public double getEWMALoad() {
    return load.getEwmaValue();
  }

  @Override
  public void updateLoad(final double value) {
    load.updateValue(value);
  }

  @Override
  public GroupMetrics getMetricHolder() {
    return metricHolder;
  }

  @Override
  public boolean isActive() {
   return operatorChainManager.size() != 0;
  }

  @Override
  public boolean isAssigned() {
    return assigned.get();
  }

  @Override
  public boolean compareAndSetAssigned(final boolean cmp, final boolean value) {
    return assigned.compareAndSet(cmp, value);
  }

  @Override
  public GroupSourceManager getGroupSourceManager() {
    return groupSourceManager;
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public String toString() {
    return groupId;
  }
}