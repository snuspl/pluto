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
package edu.snu.mist.core.task.metrics;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.*;

import javax.inject.Inject;
import java.util.Collection;

/**
 * A class handles the metric event about the number of events.
 */
public final class EventNumMetricEventHandler implements MetricTrackEventHandler {

  /**
   * The map of group ids and group info to update.
   */
  private final GroupInfoMap groupInfoMap;

  /**
   * The global metric holder.
   */
  private final MetricHolder globalMetricHolder;

  @Inject
  private EventNumMetricEventHandler(final GroupInfoMap groupInfoMap,
                                     final MetricHolder globalMetricHolder,
                                     final MistPubSubEventHandler pubSubEventHandler) {
    this.groupInfoMap = groupInfoMap;
    this.globalMetricHolder = globalMetricHolder;
    pubSubEventHandler.getPubSubEventHandler().subscribe(MetricTrackEvent.class, this);
  }

  @Override
  public void onNext(final MetricTrackEvent metricTrackEvent) {
    long totalNumEvent = 0;
    for (final GroupInfo groupInfo : groupInfoMap.values()) {
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
      final EWMAMetric numEventMetric =
          groupInfo.getMetricHolder().getNumEventsMetric();
      numEventMetric.updateValue(groupNumEvent);
      totalNumEvent += groupNumEvent;
    }
    globalMetricHolder.getNumEventsMetric().updateValue(totalNumEvent);
  }
}