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
import edu.snu.mist.core.task.globalsched.parameters.SchedulingPeriod;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * The factory class of GlobalSchedEventPrcoessor.
 */
public final class GlobalSchedEventProcessorFactory implements EventProcessorFactory {

  /**
   * The scheduling period.
   */
  private final long schedulingPeriod;

  /**
   * Scheduler of the operator chain manager.
   */
  private final GlobalScheduler scheduler;

  @Inject
  private GlobalSchedEventProcessorFactory(@Parameter(SchedulingPeriod.class) final long schedulingPeriod,
                                           final GlobalScheduler scheduler) {
    super();
    this.schedulingPeriod = schedulingPeriod;
    this.scheduler = scheduler;
  }

  @Override
  public EventProcessor newEventProcessor() {
    return new GlobalSchedEventProcessor(schedulingPeriod, scheduler);
  }
}