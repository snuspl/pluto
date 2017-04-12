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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class represents the group tracker which measures the metric of each group such as number of events.
 * It periodically check the metric and update the GroupInfo.
 */
final class GroupMetricTracker implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(GroupMetricTracker.class.getName());

  /**
   * The executor service which provide the thread pool for metric tracking.
   */
  private ScheduledExecutorService executorService;

  /**
   * The interval of group metric tracking.
   */
  private long groupTrackingInterval;

  /**
   * The future of group tracking service execution.
   */
  private ScheduledFuture result;

  @Inject
  private GroupMetricTracker(final ScheduledExecutorServiceWrapper executorServiceWrapper,
                             @Parameter(GroupTrackingInterval.class) final long groupTrackingInterval) {
    this.executorService = executorServiceWrapper.getScheduler();
    this.groupTrackingInterval = groupTrackingInterval;
  }

  /**
   * Start the group metric tracker.
   * @param groupInfoMap the map of group ids and group info to update
   */
  public void start(final GroupInfoMap groupInfoMap) {
    // Schedule a periodic group tracking job
    result = executorService.scheduleWithFixedDelay(new Runnable() {
      public void run() {
        try {
          for (final GroupInfo groupInfo : groupInfoMap.values()) {
            // Track the number of event per each group
            long numEvent = 0;
            final Set<DAG<ExecutionVertex, MISTEdge>> calculatedDag
                = new HashSet<>(groupInfo.getExecutionDags().size());
            for (final DAG<ExecutionVertex, MISTEdge> dag : groupInfo.getExecutionDags().values()) {
              if (!calculatedDag.contains(dag)) {
                calculatedDag.add(dag);

                // Traverse the DAG in DFS order and update the numEvent
                final Set<ExecutionVertex> visited = new HashSet<>(dag.numberOfVertices());
                for (final ExecutionVertex source : dag.getRootVertices()) {
                  numEvent = dfsTrack(dag, visited, source, numEvent);
                }
              }
            }
            final GroupMetric metric = groupInfo.getGroupMetric();
            metric.setNumEvents(numEvent);
          }
        } catch (final Exception e) {
          LOG.log(Level.SEVERE, e.toString());
        }
      }
    }, groupTrackingInterval, groupTrackingInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * This function sum up the total number of events in each operator chain's queue of a group in DFS order.
   * @param dag execution dag to calculate the number of events
   * @param visited a set that holds the visited vertices
   * @param currentVertex currently visited vertex
   * @param numEvent the number of events calculate before visiting this vertex
   */
  private long dfsTrack(final DAG<ExecutionVertex, MISTEdge> dag,
                        final Set<ExecutionVertex> visited,
                        final ExecutionVertex currentVertex,
                        final long numEvent) {
    // Add to the visited set
    visited.add(currentVertex);

    // The local event number value
    long localNumEvent = numEvent;

    // Traverse in DFS order
    for (final Map.Entry<ExecutionVertex, MISTEdge> neighbor : dag.getEdges(currentVertex).entrySet()) {
      final ExecutionVertex child = neighbor.getKey();
      if (child.getType() == ExecutionVertex.Type.OPERATOR_CHIAN && !visited.contains(child)) {
        localNumEvent += ((OperatorChain) child).numberOfEvents();
        if (!dag.getEdges(child).isEmpty()) {
          localNumEvent = dfsTrack(dag, visited, child, localNumEvent);
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
