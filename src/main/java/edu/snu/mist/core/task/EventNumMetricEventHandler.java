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
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Collection;

/**
 * A class handles the metric event about EventNumMetric.
 */
public final class EventNumMetricEventHandler implements EventHandler<MetricEvent> {

  /**
   * The map of group ids and group info to update.
   */
  private final GroupInfoMap groupInfoMap;

  /**
   * The global metrics.
   */
  private final GlobalMetrics globalMetrics;

  @Inject
  private EventNumMetricEventHandler(final MetricPubSubEventHandler metricPubSubEventHandler,
                                     final GroupInfoMap groupInfoMap,
                                     final GlobalMetrics globalMetrics) {
    this.groupInfoMap = groupInfoMap;
    this.globalMetrics = globalMetrics;
    metricPubSubEventHandler.getPubSubEventHandler().subscribe(MetricEvent.class, this);
  }

  @Override
  public void onNext(final MetricEvent metricEvent) {
    long totalNumEvent = 0;
    for (final GroupInfo groupInfo : groupInfoMap.values()) {
      // Track the number of event per each group
      long groupNumEvent = 0;
      for (final DAG<ExecutionVertex, MISTEdge> dag : groupInfo.getExecutionDags().getUniqueValues()) {
        final Collection<ExecutionVertex> vertices = dag.getVertices();
        for (final ExecutionVertex ev : vertices) {
          if (ev.getType() == ExecutionVertex.Type.OPERATOR_CHIAN) {
            groupNumEvent += ((OperatorChain) ev).numberOfEvents();
          }
        }
      }
      final EventNumMetric metric = groupInfo.getEventNumMetric();
      metric.updateNumEvents(groupNumEvent);
      totalNumEvent += groupNumEvent;
    }
    globalMetrics.getNumEventMetric().updateNumEvents(totalNumEvent);
  }
}