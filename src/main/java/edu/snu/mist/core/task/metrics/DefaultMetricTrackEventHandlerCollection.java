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

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Consumer;

/**
 * This is a default implementation of MetricTrackEventHandlerCollection.
 */
public final class DefaultMetricTrackEventHandlerCollection implements MetricTrackEventHandlerCollection {

  private final Collection<MetricTrackEventHandler> handlerCollection;

  @Inject
  private DefaultMetricTrackEventHandlerCollection(final EventNumMetricEventHandler metricEventHandler) {
    this.handlerCollection = new LinkedList<>();
    handlerCollection.add(metricEventHandler);
  }

  @Override
  public void forEach(final Consumer<? super MetricTrackEventHandler> action) {
    handlerCollection.forEach(action);
  }
}
