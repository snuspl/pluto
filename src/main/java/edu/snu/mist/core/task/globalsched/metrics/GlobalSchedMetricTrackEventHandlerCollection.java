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

import edu.snu.mist.core.task.metrics.MetricTrackEventHandler;
import edu.snu.mist.core.task.metrics.MetricTrackEventHandlerCollection;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Consumer;

/**
 * This is an implementation of MetricTrackEventHandlerCollection used for global scheduling.
 */
public final class GlobalSchedMetricTrackEventHandlerCollection implements MetricTrackEventHandlerCollection {

  private final Collection<MetricTrackEventHandler> handlerCollection;

  @Inject
  private GlobalSchedMetricTrackEventHandlerCollection(final EventNumAndWeightMetricEventHandler numMetricEventHandler,
                                                       final CpuUtilMetricEventHandler cpuUtilMetricEventHandler) {
    this.handlerCollection = new LinkedList<>();
    handlerCollection.add(numMetricEventHandler);
    handlerCollection.add(cpuUtilMetricEventHandler);
  }

  @Override
  public void forEach(final Consumer<? super MetricTrackEventHandler> action) {
    handlerCollection.forEach(action);
  }
}
