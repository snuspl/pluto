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

import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.task.eventProcessors.EventProcessorManager;
import edu.snu.mist.core.task.metrics.MetricHolder;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * A class which contains query and metric information about query group.
 */
public final class GroupInfo implements AutoCloseable {

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
   * An event processor manager.
   */
  private final EventProcessorManager eventProcessorManager;

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
   * A metric holder that contains the number of events in the group, which will be updated periodically.
   */
  private final MetricHolder metricHolder;

  @Inject
  private GroupInfo(@Parameter(GroupId.class) final String groupId,
                    final ExecutionDags executionDags,
                    final EventProcessorManager eventProcessorManager,
                    final QueryStarter queryStarter,
                    final OperatorChainManager operatorChainManager,
                    final QueryRemover queryRemover,
                    final MetricHolder metricHolder) {
    this.groupId = groupId;
    this.queryIdList = new ArrayList<>();
    this.executionDags = executionDags;
    this.eventProcessorManager = eventProcessorManager;
    this.queryStarter = queryStarter;
    this.operatorChainManager = operatorChainManager;
    this.queryRemover = queryRemover;
    this.metricHolder = metricHolder;
  }

  /**
   * Add a query id into this group.
   * @param queryId the query id to add
   */
  public void addQueryIdToGroup(final String queryId) {
    queryIdList.add(queryId);
  }

  /**
   * Get the query starter.
   * @return query starter
   */
  public QueryStarter getQueryStarter() {
    return queryStarter;
  }

  /**
   * Get the event processor manager.
   * @return event processor manager
   */
  public EventProcessorManager getEventProcessorManager() {
    return eventProcessorManager;
  }

  /**
   * @return the list of query id in this group
   */
  public List<String> getQueryIdList() {
    return queryIdList;
  }

  /**
   * @return the metric holder contains the number of events in this group
   */
  public MetricHolder getMetricHolder() {
    return metricHolder;
  }

  /**
   * Return the execution dags in the group.
   * @return execution dags
   */
  public ExecutionDags getExecutionDags() {
    return executionDags;
  }

  /**
   * Return the query remover.
   * @return query remover
   */
  public QueryRemover getQueryRemover() {
    return queryRemover;
  }

  @Override
  public void close() throws Exception {
    eventProcessorManager.close();
  }

  @Override
  public String toString() {
    return groupId;
  }
}