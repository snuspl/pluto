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
import edu.snu.mist.core.parameters.DefaultGroupWeight;
import edu.snu.mist.core.task.eventProcessors.EventProcessorManager;
import org.apache.reef.tang.annotations.Parameter;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * A class which contains query and metric information about query group.
 */
final class GroupInfo implements AutoCloseable {

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
  private final ExecutionDags<String> executionDags;

  /**
   * A group metric which will be updated periodically.
   */
  private final GroupMetric groupMetric;

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
   * The weight of the group.
   */
  private int weight;

  @Inject
  private GroupInfo(@Parameter(GroupId.class) final String groupId,
                    @Parameter(DefaultGroupWeight.class) final int weight,
                    final GroupMetric groupMetric,
                    final ExecutionDags<String> executionDags,
                    final EventProcessorManager eventProcessorManager,
                    final QueryStarter queryStarter,
                    final OperatorChainManager operatorChainManager,
                    final QueryRemover queryRemover) {
    this.groupId = groupId;
    this.weight = weight;
    this.queryIdList = new ArrayList<>();
    this.executionDags = executionDags;
    this.groupMetric = groupMetric;
    this.eventProcessorManager = eventProcessorManager;
    this.queryStarter = queryStarter;
    this.operatorChainManager = operatorChainManager;
    this.queryRemover = queryRemover;
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
   * @return the metric of this group
   */
  public GroupMetric getGroupMetric() {
    return groupMetric;
  }

  /**
   * Return the execution dags in the group.
   * @return execution dags
   */
  public ExecutionDags<String> getExecutionDags() {
    return executionDags;
  }

  /**
   * Return the query remover.
   * @return query remover
   */
  public QueryRemover getQueryRemover() {
    return queryRemover;
  }

  /**
   * Get the weight of the group.
   * @return weight
   */
  public int getWeight() {
    return weight;
  }

  /**
   * Set the weight of the group.
   * @param w weight
   */
  public void setWeight(final int w) {
    weight = w;
  }

  @Override
  public void close() throws Exception {
    eventProcessorManager.close();
  }
}