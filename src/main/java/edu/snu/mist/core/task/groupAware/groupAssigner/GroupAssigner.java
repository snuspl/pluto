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
package edu.snu.mist.core.task.groupAware.groupAssigner;

import edu.snu.mist.core.task.groupAware.Group;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * It assigns a group to an event processor.
 */
@DefaultImplementation(MinLoadGroupAssignerImpl.class)
public interface GroupAssigner {

  /**
   * Assign a group to an event processor.
   * @param newGroup new group
   */
  void assignGroup(Group newGroup);

  /**
   * This should be called once when the groupAllocationTable is initialized.
   */
  void initialize();
}
