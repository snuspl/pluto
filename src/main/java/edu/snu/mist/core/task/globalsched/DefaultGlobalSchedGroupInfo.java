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
import edu.snu.mist.core.task.deactivation.GroupSourceManager;
import edu.snu.mist.core.task.globalsched.parameters.DefaultGroupLoad;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is the default implementation of the GlobalSchedGroupInfo.
 */
final class DefaultGlobalSchedGroupInfo implements GlobalSchedGroupInfo {

  /**
   * Group status.
   */
  private enum GroupStatus {
    READY,
    DISPATCHED,
    PROCESSING,
    ISOLATED,
  }

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
   * GroupSourceManager for this group.
   */
  private final GroupSourceManager groupSourceManager;

  /**
   * The load of the group.
   */
  private double groupLoad;

  /**
   * The event processing time of the group.
   */
  private final AtomicLong totalProcessingTime;

  /**
   * The number of processed events in the group.
   */
  private final AtomicLong totalProcessingEvent;

  /**
   * Group status.
   */
  private final AtomicReference<GroupStatus> atomicStatus;

  /**
   * The latest rebalance time.
   */
  private long latestRebalanceTime;

  @Inject
  private DefaultGlobalSchedGroupInfo(@Parameter(GroupId.class) final String groupId,
                                      @Parameter(DefaultGroupLoad.class) final double defaultLoad,
                                      final ExecutionDags executionDags,
                                      final QueryStarter queryStarter,
                                      final OperatorChainManager operatorChainManager,
                                      final QueryRemover queryRemover,
                                      final GroupSourceManager groupSourceManager) {
    this.groupId = groupId;
    this.groupLoad = defaultLoad;
    this.queryIdList = new ArrayList<>();
    this.executionDags = executionDags;
    this.queryStarter = queryStarter;
    this.operatorChainManager = operatorChainManager;
    this.queryRemover = queryRemover;
    this.groupSourceManager = groupSourceManager;
    this.totalProcessingTime = new AtomicLong(0);
    this.totalProcessingEvent = new AtomicLong(0);
    this.atomicStatus = new AtomicReference<>(GroupStatus.READY);
    this.latestRebalanceTime = System.nanoTime();
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
  public long numberOfRemainingEvents() {
    return operatorChainManager.numEvents();
  }

  @Override
  public double getLoad() {
    return groupLoad;
  }

  @Override
  public void setLoad(final double load) {
    groupLoad = load;
  }

  @Override
  public void setLatestRebalanceTime(final long rebalanceTime) {
    latestRebalanceTime = rebalanceTime;
  }

  @Override
  public long getLatestRebalanceTime() {
    return latestRebalanceTime;
  }

  @Override
  public boolean isActive() {
   return operatorChainManager.size() != 0;
  }

  @Override
  public GroupSourceManager getGroupSourceManager() {
    return groupSourceManager;
  }

  @Override
  public String getGroupId() {
    return groupId;
  }

  @Override
  public AtomicLong getProcessingTime() {
    return totalProcessingTime;
  }

  @Override
  public AtomicLong getProcessingEvent() {
    return totalProcessingEvent;
  }

  @Override
  public boolean setDispatched() {
    return atomicStatus.compareAndSet(GroupStatus.READY, GroupStatus.DISPATCHED);
  }

  @Override
  public boolean setProcessing() {
    return atomicStatus.compareAndSet(GroupStatus.DISPATCHED, GroupStatus.PROCESSING);
  }

  @Override
  public boolean setIsolated() {
    return atomicStatus.compareAndSet(GroupStatus.PROCESSING, GroupStatus.ISOLATED);
  }

  @Override
  public boolean setReadyFromProcessing() {
    return atomicStatus.compareAndSet(GroupStatus.PROCESSING, GroupStatus.READY);
  }

  @Override
  public boolean setReadyFromDispatched() {
    return atomicStatus.compareAndSet(GroupStatus.DISPATCHED, GroupStatus.READY);
  }

  @Override
  public boolean setReadyFromIsolated() {
    return atomicStatus.compareAndSet(GroupStatus.ISOLATED, GroupStatus.READY);
  }

  @Override
  public boolean isProcessing() {
    return atomicStatus.get() == GroupStatus.PROCESSING;
  }

  @Override
  public boolean isReady() {
    return atomicStatus.get() == GroupStatus.READY;
  }

  @Override
  public boolean isDispatched() {
    return atomicStatus.get() == GroupStatus.DISPATCHED;
  }

  @Override
  public boolean isIsolated() {
    return atomicStatus.get() == GroupStatus.ISOLATED;
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(groupId);
    sb.append(", ");
    sb.append(atomicStatus.get());
    sb.append(", ");
    sb.append(groupLoad);
    sb.append(", ");
    sb.append(numberOfRemainingEvents());
    sb.append("}");
    return sb.toString();
  }
}