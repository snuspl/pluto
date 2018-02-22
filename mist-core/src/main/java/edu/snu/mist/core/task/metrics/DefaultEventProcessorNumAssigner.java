/*
 * Copyright (C) 2018 Seoul National University
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

import edu.snu.mist.core.task.MistPubSubEventHandler;

import javax.inject.Inject;

/**
 * This is a EventProcessorNumAssigner assigns that does not change the number of event processors.
 */
public final class DefaultEventProcessorNumAssigner implements EventProcessorNumAssigner {


  @Inject
  private DefaultEventProcessorNumAssigner(final MistPubSubEventHandler pubSubEventHandler) {
    pubSubEventHandler.getPubSubEventHandler().subscribe(MetricUpdateEvent.class, this);
  }

  @Override
  public void onNext(final MetricUpdateEvent metricUpdateEvent) {
    // do nothing
  }
}
