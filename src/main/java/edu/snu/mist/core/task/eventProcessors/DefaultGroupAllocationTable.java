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

import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class DefaultGroupAllocationTable implements GroupAllocationTable {

  private final List<EventProcessor> eventProcessors;
  private final ConcurrentMap<EventProcessor, Collection<GlobalSchedGroupInfo>> table;

  @Inject
  private DefaultGroupAllocationTable() {
    this.eventProcessors = new CopyOnWriteArrayList<>();
    this.table = new ConcurrentHashMap<>();
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
  public Collection<GlobalSchedGroupInfo> remove(final EventProcessor key) {
    return table.remove(key);
  }
}
