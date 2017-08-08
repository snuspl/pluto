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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class DefaultGroupAllocationTable implements GroupAllocationTable {

  private final List<EventProcessor> eventProcessors;
  private final ConcurrentMap<EventProcessor, Collection<GlobalSchedGroupInfo>> table;

  @Inject
  private DefaultGroupAllocationTable(
      @Parameter(DefaultNumEventProcessors.class) final int defaultNumEventProcessors,
      final EventProcessorFactory eventProcessorFactory) {
    this.eventProcessors = new CopyOnWriteArrayList<>();
    this.table = new ConcurrentHashMap<>();
    // Create event processors
    for (int i = 0; i < defaultNumEventProcessors; i++) {
      final EventProcessor eventProcessor = eventProcessorFactory.newEventProcessor();
      put(eventProcessor, new ConcurrentLinkedQueue<>());
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
  public void put(final EventProcessor key, final Collection<GlobalSchedGroupInfo> value) {
    table.put(key, value);
    eventProcessors.add(key);
  }

  @Override
  public int size() {
    return eventProcessors.size();
  }

  @Override
  public void addEventProcessor(final EventProcessor eventProcessor) {
    table.put(eventProcessor, new ConcurrentLinkedQueue<>());
    eventProcessors.add(eventProcessor);
  }

  @Override
  public void removeEventProcessor(final EventProcessor eventProcessor) {
    eventProcessors.remove(eventProcessor);
    table.remove(eventProcessor);
  }

  @Override
  public Collection<GlobalSchedGroupInfo> remove(final EventProcessor key) {
    return table.remove(key);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (final EventProcessor eventProcessor : eventProcessors) {
      final Collection<GlobalSchedGroupInfo> groups = table.get(eventProcessor);
      sb.append(eventProcessor);
      sb.append(" -> [");
      sb.append(groups.size());
      sb.append("], ");
      sb.append(table.get(eventProcessor));
      sb.append("\n");
    }
    return sb.toString();
  }
}