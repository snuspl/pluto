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

import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * This is a default implementation that can adjust the number of event processors.
 * This will remove event processors randomly when it will decrease the number of event processors.
 */
public final class DefaultEventProcessorManager implements EventProcessorManager {

  /**
   * A set of EventProcessor.
   */
  private final Set<EventProcessor> eventProcessors;

  /**
   * Event processor factory.
   */
  private final EventProcessorFactory eventProcessorFactory;

  @Inject
  private DefaultEventProcessorManager(@Parameter(DefaultNumEventProcessors.class) final int defaultNumEventProcessors,
                                       final EventProcessorFactory eventProcessorFactory) {
    this.eventProcessors = new HashSet<>();
    this.eventProcessorFactory = eventProcessorFactory;
    addNewThreadsToSet(defaultNumEventProcessors);
  }

  /**
   * Create new event processors and add them to event processor set.
   * @param numToCreate the number of processors to create
   */
  private void addNewThreadsToSet(final long numToCreate) {
    for (int i = 0; i < numToCreate; i++) {
      final EventProcessor eventProcessor = eventProcessorFactory.newEventProcessor();
      eventProcessors.add(eventProcessor);
      eventProcessor.start();
    }
  }

  @Override
  public void adjustEventProcessorNum(final long threadNum) {
    final int currentThreadNum = eventProcessors.size();
    if (currentThreadNum <= threadNum) {
      // if we need to make more event processor
      addNewThreadsToSet(threadNum - currentThreadNum);
    } else if (currentThreadNum > threadNum) {
      // if we need to close some processor
      int closedProcessorNum = 0;
      final Iterator<EventProcessor> iterator = eventProcessors.iterator();
      while(iterator.hasNext()) {
        final EventProcessor eventProcessor = iterator.next();
        eventProcessor.close();
        iterator.remove();
        closedProcessorNum++;
        if (closedProcessorNum >= currentThreadNum - threadNum) {
          break;
        }
      }
    }
  }

  @Override
  public Set<EventProcessor> getEventProcessors() {
    return eventProcessors;
  }

  @Override
  public void close() throws Exception {
    eventProcessors.forEach(eventProcessor -> eventProcessor.close());
  }
}
