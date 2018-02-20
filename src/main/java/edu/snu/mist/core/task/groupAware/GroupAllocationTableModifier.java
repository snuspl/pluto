
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
package edu.snu.mist.core.task.groupaware;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * A single writer thread that modifies the group allocation table.
 * We use a single writer in order to reduce concurrent modification of the group allocation table.
 * By doing so, we can guarantee that only a single thread modifies the group allocation table.
 */
@DefaultImplementation(GroupAllocationTableModifierImpl.class)
public interface GroupAllocationTableModifier extends AutoCloseable {

  /**
   * Add an event that modifies the group allocation table.
   */
  void addEvent(WritingEvent event);
}