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
package edu.snu.mist.core.task.utils;

import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.eventProcessors.EventProcessorManager;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;

/**
 * This class represents a simple EventProcessorManager for test.
 * It just keep the processorNum value.
 */
public final class TestEventProcessorManager implements EventProcessorManager {

  private long processorNum;

  public TestEventProcessorManager() {
    this.processorNum = 0;
  }

  @Override
  public Set<EventProcessor> getEventProcessors() {
    final Set<EventProcessor> set = new HashSet<>();
    for (int i = 0; i < processorNum; i++) {
      final EventProcessor processor = mock(EventProcessor.class);
      set.add(processor);

    }
    return set;
  }

  @Override
  public void adjustEventProcessorNum(final long adjustNum) {
    this.processorNum = adjustNum;
  }

  @Override
  public void close() {
    // do nothing
  }
}
