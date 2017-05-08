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

import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.eventProcessors.EventProcessorFactory;

import javax.inject.Inject;

/**
 * The factory class creates a group active event processor that shares a global RB-tree based group selector.
 */
public final class GroupActivationSharedTreeEventProcessorFactory implements EventProcessorFactory {


  /**
   * Scheduling period calculator.
   */
  private final SchedulingPeriodCalculator schedulingPeriodCalculator;

  private final NextGroupSelector nextGroupSelector;
  @Inject
  private GroupActivationSharedTreeEventProcessorFactory(
      final NextGroupSelector nextGroupSelector,
      final SchedulingPeriodCalculator schedulingPeriodCalculator) {
    this.nextGroupSelector = nextGroupSelector;
    this.schedulingPeriodCalculator = schedulingPeriodCalculator;
  }

  @Override
  public EventProcessor newEventProcessor() {
    return new GroupActivationEventProcessor(schedulingPeriodCalculator, nextGroupSelector);
  }
}