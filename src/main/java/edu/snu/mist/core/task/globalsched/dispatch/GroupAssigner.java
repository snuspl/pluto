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
package edu.snu.mist.core.task.globalsched.dispatch;

import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

/**
 * This is an interface that assigns groups to next group selectors.
 */
@DefaultImplementation(ModGroupAssigner.class)
public interface GroupAssigner extends EventHandler<GroupEvent> {

  /**
   * Assign the group to a next group selector (event processor).
   * @param groupInfo group info
   */
  void assign(GlobalSchedGroupInfo groupInfo);

  /**
   * Add a new group selector (event processor).
   */
  void addGroupSelector(NextGroupSelector groupSelector);

  /**
   * Remove the existing group selector (event processor).
   */
  void removeGroupSelector(NextGroupSelector groupSelector);
}