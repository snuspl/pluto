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
package edu.snu.mist.core.task.eventProcessors;

import edu.snu.mist.core.task.OperatorChainManager;
import edu.snu.mist.core.task.eventProcessors.parameters.EventProcessorPollingInterval;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * This is a polling event processor factory.
 */
public final class PollingEventProcessorFactory implements EventProcessorFactory {

  /**
   * The polling interval when the thread wakes up and polls whether there is an event
   * or not.
   */
  private final int pollingIntervalMillisecond;

  /**
   * The operator chain manager.
   */
  private final OperatorChainManager operatorChainManager;


  @Inject
  private PollingEventProcessorFactory(
      @Parameter(EventProcessorPollingInterval.class) final int pollingIntervalMillisecond,
      final OperatorChainManager operatorChainManager) {
    this.pollingIntervalMillisecond = pollingIntervalMillisecond;
    this.operatorChainManager = operatorChainManager;
  }

  @Override
  public EventProcessor newEventProcessor() {
    return new PollingEventProcessor(pollingIntervalMillisecond, operatorChainManager);
  }
}