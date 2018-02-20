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
package edu.snu.mist.core.task.groupaware;

import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessorFactory;
import edu.snu.mist.core.task.groupaware.eventprocessor.NextGroupSelector;
import edu.snu.mist.core.task.groupaware.eventprocessor.NextGroupSelectorFactory;
import edu.snu.mist.core.task.groupaware.parameters.ProcessingTimeout;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The factory class of GlobalSchedNonBlockingEventPrcoessor.
 */
public final class GlobalSchedNonBlockingEventProcessorFactory implements EventProcessorFactory {

  /**
   * Next group selector factory.
   */
  private final NextGroupSelectorFactory nextGroupSelectorFactory;

  private final AtomicInteger id = new AtomicInteger(0);

  /**
   * Processing timeout.
   */
  private final long timeout;

  @Inject
  private GlobalSchedNonBlockingEventProcessorFactory(
      final NextGroupSelectorFactory nextGroupSelectorFactory,
      @Parameter(ProcessingTimeout.class) final long timeout) {
    this.nextGroupSelectorFactory = nextGroupSelectorFactory;
    this.timeout = timeout;
  }

  @Override
  public EventProcessor newEventProcessor() {
    final NextGroupSelector nextGroupSelector = nextGroupSelectorFactory.newInstance();
    return new GlobalSchedNonBlockingEventProcessor(nextGroupSelector, id.getAndIncrement(), timeout);
  }
}