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
   * This manager should be a BlockingActiveOperatorChainManager because of the ConditionEventProcessor.
   */
  private final BlockingActiveOperatorChainPickManager operatorChainManager;

  /**
   * A set of EventProcessor.
   */
  private final Set<EventProcessor> eventProcessors;

  @Inject
  private DynamicThreadManager(@Parameter(NumThreads.class) final int numThreads,
                               final BlockingActiveOperatorChainPickManager operatorChainManager) {
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
      final EventProcessor eventProcessor = new ConditionEventProcessor(operatorChainManager);
      eventProcessors.add(eventProcessor);
      eventProcessor.start();
    }
  }

  @Override
  public void setThreadNum(final int threadNum) {
    final int currentThreadNum = eventProcessors.size();
    if (currentThreadNum <= threadNum) {
      // if we need to make more event processor
      addNewThreadsToSet(threadNum - currentThreadNum);
    } else if (currentThreadNum > threadNum) {
      // if we need to reap some processor
      final Set<EventProcessor> processorsToBeReaped = new HashSet<>();
      final Iterator<EventProcessor> iterator = eventProcessors.iterator();
      for (int i = 0; i < currentThreadNum - threadNum && iterator.hasNext(); i++) {
        // put this processor to the set of processors to be reaped
        processorsToBeReaped.add(iterator.next());
      }
      // stop managing these processors and mark them to be reaped after their current execution
      processorsToBeReaped.forEach((eventProcessor) -> {
        eventProcessors.remove(eventProcessor);
        eventProcessor.setToBeReaped();
      });
    }
  }

  @Override
  public Set<EventProcessor> getEventProcessors() {
    return eventProcessors;
  }

  @Override
  public void close() throws Exception {
    for (final EventProcessor eventProcessor : eventProcessors) {
      eventProcessor.interrupt();
    }
  }
}
