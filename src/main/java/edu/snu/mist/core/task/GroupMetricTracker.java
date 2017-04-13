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

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.parameters.GroupTrackingInterval;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * This class represents the group tracker which measures the metric of each group such as number of events.
 * It periodically check the metric and update the GroupInfo.
 */
final class GroupMetricTracker implements AutoCloseable {

  /**
   * The executor service which provide the thread pool for metric tracking.
   */
  private final ScheduledExecutorService executorService;

  /**
   * The interval of group metric tracking.
   */
  private final long groupTrackingInterval;

  /**
   * The future of group tracking service execution.
   */
  private ScheduledFuture result;

  /**
   * The map of group ids and group info to update.
   */
  private final GroupInfoMap groupInfoMap;

  /**
   * The group resource orchestrator which manages resources assigned to each group.
   */
  private final GroupResourceOrchestrator orchestrator;

  @Inject
  private GroupMetricTracker(final GroupTrackerExecutorServiceWrapper executorServiceWrapper,
                             @Parameter(GroupTrackingInterval.class) final long groupTrackingInterval,
                             final GroupInfoMap groupInfoMap,
                             final GroupResourceOrchestrator orchestrator) {
    this.executorService = executorServiceWrapper.getScheduler();
    this.groupTrackingInterval = groupTrackingInterval;
    this.groupInfoMap = groupInfoMap;
    this.orchestrator = orchestrator;
  }

  /**
   * Start the group metric tracker.
   */
  public void start() {
    // Schedule a periodic group tracking job
    result = executorService.scheduleWithFixedDelay(new Runnable() {
      public void run() {
        try {
          for (final GroupInfo groupInfo : groupInfoMap.values()) {
            // Track the number of event per each group
            long numEvent = 0;
            for (final DAG<ExecutionVertex, MISTEdge> dag : groupInfo.getExecutionDags().getUniqueValues()) {
              // Traverse the DAG in DFS order and update the numEvent
              final Set<ExecutionVertex> visited = new HashSet<>(dag.numberOfVertices());
              for (final ExecutionVertex source : dag.getRootVertices()) {
                numEvent += getNumEventUsingDfs(dag, visited, source);
              }
            }
            final GroupMetric metric = groupInfo.getGroupMetric();
            metric.setNumEvents(numEvent);
          }
          orchestrator.groupMetricUpdated();
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }, groupTrackingInterval, groupTrackingInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * This method sums up the total number of events in each operator chain's queue of a group in DFS order.
   * @param dag execution dag to calculate the number of events
   * @param visited a set that holds the visited vertices
   * @param currentVertex currently visited vertex
   */
  private long getNumEventUsingDfs(final DAG<ExecutionVertex, MISTEdge> dag,
                                   final Set<ExecutionVertex> visited,
                                   final ExecutionVertex currentVertex) {
    // Add to the visited set
    visited.add(currentVertex);

    // The local event number value
    long localNumEvent = 0;

    // Traverse in DFS order
    for (final Map.Entry<ExecutionVertex, MISTEdge> neighbor : dag.getEdges(currentVertex).entrySet()) {
      final ExecutionVertex child = neighbor.getKey();
      if (child.getType() == ExecutionVertex.Type.OPERATOR_CHIAN && !visited.contains(child)) {
        localNumEvent += ((OperatorChain) child).numberOfEvents();
        if (!dag.getEdges(child).isEmpty()) {
          localNumEvent += getNumEventUsingDfs(dag, visited, child);
        }
      }
    }

    return localNumEvent;
  }

  @Override
  public void close() throws Exception {
    result.cancel(true);
    executorService.shutdown();
  }
}