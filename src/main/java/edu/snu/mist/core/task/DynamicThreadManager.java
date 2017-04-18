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
package edu.snu.mist.core.task;

import edu.snu.mist.core.parameters.NumThreads;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * This class manages a dynamic number of threads.
 */
public final class DynamicThreadManager implements ThreadManager {

  /**
   * The operator chain manager.
   */
  private final OperatorChainManager operatorChainManager;

  /**
   * A set of EventProcessor.
   */
  private final Set<EventProcessor> eventProcessors;

  @Inject
  private DynamicThreadManager(@Parameter(NumThreads.class) final int numThreads,
                               final OperatorChainManager operatorChainManager) {
    this.operatorChainManager = operatorChainManager;
    this.eventProcessors = new HashSet<>();
    addNewThreadsToSet(numThreads);
  }

  /**
   * Create new event processors and add them to event processor set.
   * @param numToCreate the number of processors to create
   */
  private void addNewThreadsToSet(final int numToCreate) {
    for (int i = 0; i < numToCreate; i++) {
      final EventProcessor eventProcessor =
          new ConditionEventProcessor((BlockingActiveOperatorChainPickManager) operatorChainManager);
      eventProcessors.add(eventProcessor);
      eventProcessor.start();
    }
  }

  @Override
  public void adjustThreadNum(final int threadNum) {
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
        try {
          eventProcessor.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
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
    for (final EventProcessor eventProcessor : eventProcessors) {
      eventProcessor.close();
    }
  }
}
