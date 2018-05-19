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
package edu.snu.mist.core.task.groupaware.eventprocessor;

import edu.snu.mist.core.task.groupaware.parameters.ProcessingTimeout;
import net.openhft.affinity.AffinityThreadFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * The factory class of AffinityEventProcessor.
 */
public final class AffinityEventProcessorFactory implements EventProcessorFactory {
  private static final Logger LOG = Logger.getLogger(AffinityEventProcessorFactory.class.getName());

  /**
   * Next group selector factory.
   */
  private final NextGroupSelectorFactory nextGroupSelectorFactory;

  /**
   * Thread id.
   */
  private final AtomicInteger id = new AtomicInteger(0);

  /**
   * Affinity thread factory.
   */
  private final AffinityThreadFactory affinityThreadFactory;

  /**
   * Processing timeout.
   */
  private final long timeout;
  @Inject
  private AffinityEventProcessorFactory(final NextGroupSelectorFactory nextGroupSelectorFactory,
                                        @Parameter(ProcessingTimeout.class) final long timeout) {
    this.nextGroupSelectorFactory = nextGroupSelectorFactory;
    this.timeout = timeout;
    this.affinityThreadFactory = new AffinityThreadFactory("Afnty");
    LOG.info("AffinityEventProcessorFactory start");
  }

  @Override
  public EventProcessor newEventProcessor() {
    final NextGroupSelector nextGroupSelector = nextGroupSelectorFactory.newInstance();
    final AffinityRunnable runnable = new AffinityRunnable(nextGroupSelector, timeout);
    final Thread thread = affinityThreadFactory.newThread(runnable);
    return new AffinityEventProcessor(id.getAndIncrement(), thread, runnable);
  }
}