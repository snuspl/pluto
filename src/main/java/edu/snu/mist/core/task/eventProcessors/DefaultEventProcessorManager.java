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
import edu.snu.mist.core.task.eventProcessors.parameters.EventProcessorLowerBound;
import edu.snu.mist.core.task.eventProcessors.parameters.EventProcessorUpperBound;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a default implementation that can adjust the number of event processors.
 * This will remove event processors randomly when it will decrease the number of event processors.
 */
public final class DefaultEventProcessorManager implements EventProcessorManager {

  private static final Logger LOG = Logger.getLogger(DefaultEventProcessorManager.class.getName());

  /**
   * The lowest number of event processors.
   */
  private final int eventProcessorLowerBound;

  /**
   * The highest number of event processors.
   */
  private final int eventProcessorUpperBound;

  /**
   * A set of EventProcessor.
   */
  private final Queue<EventProcessor> eventProcessors;

  /**
   * Event processor factory.
   */
  private final EventProcessorFactory eventProcessorFactory;

  @Inject
  private DefaultEventProcessorManager(@Parameter(DefaultNumEventProcessors.class) final int defaultNumEventProcessors,
                                       @Parameter(EventProcessorLowerBound.class) final int eventProcessorLowerBound,
                                       @Parameter(EventProcessorUpperBound.class) final int eventProcessorUpperBound,
                                       final EventProcessorFactory eventProcessorFactory) {
    this.eventProcessorLowerBound = eventProcessorLowerBound;
    this.eventProcessorUpperBound = eventProcessorUpperBound;
    this.eventProcessors = new LinkedList<>();
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
  public void increaseEventProcessors(final int delta) {
    if (delta < 0) {
      throw new RuntimeException("The delta value should be greater than zero, but " + delta);
    }

    synchronized (eventProcessors) {
      final int currNum = eventProcessors.size();
      final int increaseNum = Math.min(delta, eventProcessorUpperBound - currNum);
      if (increaseNum != 0) {

        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "Increase event processors from {0} to {1}",
              new Object[]{currNum, currNum + increaseNum});
        }

        addNewThreadsToSet(increaseNum);
      }
    }
  }

  @Override
  public void decreaseEventProcessors(final int delta) {
    if (delta < 0) {
      throw new RuntimeException("The delta value should be greater than zero, but " + delta);
    }

    synchronized (eventProcessors) {
      final int currNum = eventProcessors.size();
      final int decreaseNum = Math.min(delta, currNum - eventProcessorLowerBound);
      if (decreaseNum != 0) {

        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "Decrease event processors from {0} to {1}",
              new Object[]{currNum, currNum - decreaseNum});
        }

        for (int i = 0; i < decreaseNum; i++) {
          final EventProcessor eventProcessor = eventProcessors.poll();
          try {
            eventProcessor.close();
          } catch (final Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  @Override
  public void adjustEventProcessorNum(final int adjustNum) {
    if (adjustNum < 0) {
      throw new RuntimeException("The adjustNum value should be greater than zero, but " + adjustNum);
    }

    synchronized (eventProcessors) {
      final int currSize = eventProcessors.size();
      if (adjustNum < currSize) {
        decreaseEventProcessors(currSize - adjustNum);
      } else if (adjustNum > currSize) {
        increaseEventProcessors(adjustNum - currSize);
      }
    }
  }

  @Override
  public int size() {
    synchronized (eventProcessors) {
      return eventProcessors.size();
    }
  }

  @Override
  public void close() throws Exception {
    synchronized (eventProcessors) {
      eventProcessors.forEach(eventProcessor -> {
        try {
          eventProcessor.close();
        } catch (final Exception e) {
          e.printStackTrace();
        }
      });
    }
  }
}
