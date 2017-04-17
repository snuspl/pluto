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
      final Set<EventProcessor> processorsToBeClosed = new HashSet<>();
      for (final EventProcessor eventProcessor : eventProcessors) {
        // put this processor to the set of processors to be closed
        processorsToBeClosed.add(eventProcessor);
        eventProcessor.close();
        if (processorsToBeClosed.size() >= currentThreadNum - threadNum) {
          break;
        }
      }
      // stop managing these processors and mark them as closed
      processorsToBeClosed.forEach(eventProcessor -> eventProcessors.remove(eventProcessor));
    }
  }

  @Override
  public Set<EventProcessor> getEventProcessors() {
    return eventProcessors;
  }

  @Override
  public void close() throws Exception {
    eventProcessors.forEach(eventProcessor -> eventProcessor.interrupt());
  }
}
