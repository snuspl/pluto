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
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class DefaultGroupAllocationTable implements GroupAllocationTable {

  private final List<EventProcessor> eventProcessors;
  private final ConcurrentMap<EventProcessor, Collection<GlobalSchedGroupInfo>> table;
  private final int defaultNumEventProcessors;

  @Inject
  private DefaultGroupAllocationTable(
      @Parameter(DefaultNumEventProcessors.class) final int defaultNumEventProcessors,
      final EventProcessorFactory eventProcessorFactory) {
    this.eventProcessors = new CopyOnWriteArrayList<>();
    this.defaultNumEventProcessors = defaultNumEventProcessors;
    this.table = new ConcurrentHashMap<>();
    // Create event processors
    for (int i = 0; i < defaultNumEventProcessors; i++) {
      final EventProcessor eventProcessor = eventProcessorFactory.newEventProcessor();
      put(eventProcessor);
      eventProcessor.start();
    }
  }

  @Override
  public List<EventProcessor> getKeys() {
    return eventProcessors;
  }

  @Override
  public Collection<GlobalSchedGroupInfo> getValue(final EventProcessor eventProcessor) {
    return table.get(eventProcessor);
  }

  @Override
  public void put(final EventProcessor key) {
    table.put(key, new ConcurrentLinkedQueue<>());
    eventProcessors.add(key);
  }

  @Override
  public int size() {
    return eventProcessors.size();
  }

  @Override
  public List<EventProcessor> getEventProcessorsNotRunningIsolatedGroup() {
    if (defaultNumEventProcessors == eventProcessors.size()) {
      return eventProcessors;
    }

    final ArrayList<EventProcessor> normalEventProcessors = new ArrayList<>(defaultNumEventProcessors);
    for (final EventProcessor ep : eventProcessors) {
      if (!ep.isRunningIsolatedGroup()) {
        normalEventProcessors.add(ep);
      }
    }
    return normalEventProcessors;
  }

  @Override
  public Collection<GlobalSchedGroupInfo> remove(final EventProcessor key) {
    eventProcessors.remove(key);
    return table.remove(key);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (final EventProcessor ep : eventProcessors) {
      final Collection<GlobalSchedGroupInfo> groups = getValue(ep);
      sb.append(ep);
      sb.append(" -> [");
      sb.append(groups.size());
      sb.append("], ");
      sb.append(groups);
      sb.append("\n");
    }
    return sb.toString();
  }
}