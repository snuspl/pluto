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
package edu.snu.mist.core.task.globalsched.metrics;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfoMap;
import edu.snu.mist.core.task.metrics.MetricTrackEvent;
import edu.snu.mist.core.task.metrics.MetricTrackEventHandler;

import javax.inject.Inject;
import java.util.Collection;
import java.util.logging.Logger;

/**
 * A class handles the metric event about EventNumMetric.
 */
public final class EventNumAndWeightMetricEventHandler implements MetricTrackEventHandler {
  private static final Logger LOG = Logger.getLogger(EventNumAndWeightMetricEventHandler.class.getName());

  /**
   * The map of group ids and group info to update.
   */
  private final GlobalSchedGroupInfoMap groupInfoMap;

  /**
   * The global metrics.
   */
  private final GlobalSchedGlobalMetrics globalMetrics;

  @Inject
  private EventNumAndWeightMetricEventHandler(final GlobalSchedGroupInfoMap groupInfoMap,
                                              final GlobalSchedGlobalMetrics globalMetrics,
                                              final MistPubSubEventHandler pubSubEventHandler) {
    this.groupInfoMap = groupInfoMap;
    this.globalMetrics = globalMetrics;
    // Initialize
    this.onNext(new MetricTrackEvent());
    pubSubEventHandler.getPubSubEventHandler().subscribe(MetricTrackEvent.class, this);
  }

  @Override
  public void onNext(final MetricTrackEvent metricTrackEvent) {
    long totalNumEvent = 0;
    double totalWeight = 0;
    final Collection<GlobalSchedGroupInfo> groupInfos = groupInfoMap.values();
    for (final GlobalSchedGroupInfo groupInfo : groupInfos) {
      // Track the number of event per each group
      long groupNumEvent = 0;
      for (final DAG<ExecutionVertex, MISTEdge> dag : groupInfo.getExecutionDags().values()) {
        final Collection<ExecutionVertex> vertices = dag.getVertices();
        for (final ExecutionVertex ev : vertices) {
          if (ev.getType() == ExecutionVertex.Type.OPERATOR_CHAIN) {
            groupNumEvent += ((OperatorChain) ev).numberOfEvents();
          }
        }
      }
      final EventNumAndWeightMetric metric = groupInfo.getEventNumAndWeightMetric();
      metric.updateNumEvents(groupNumEvent);
      totalNumEvent += groupNumEvent;

      // Set the weight per group
      final double weight = metric.getEwmaNumEvents();
      metric.setWeight(weight);
      totalWeight += weight;
    }
    globalMetrics.getNumEventAndWeightMetric().updateNumEvents(totalNumEvent);
    globalMetrics.getNumEventAndWeightMetric().setWeight(totalWeight);
  }
}