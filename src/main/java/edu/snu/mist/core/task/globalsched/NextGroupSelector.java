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
package edu.snu.mist.core.task.globalsched;
import edu.snu.mist.core.task.globalsched.cfs.VtimeBasedNextGroupSelector;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

/**
 * This is an interface that picks a next group for processing queries.
 */
@DefaultImplementation(VtimeBasedNextGroupSelector.class)
public interface NextGroupSelector extends EventHandler<GroupEvent> {

  /**
   * Select the next group that will be processed.
   * The events of queries within the group will be executed.
   * The group info should have non-blocking operator chain manager
   * in order to reselect another operator chain manager when there are no active operator chain managers.
   * @return group info that will be executed next
   */
  GlobalSchedGroupInfo getNextExecutableGroup();

  /**
   * Re-schedule the group to the selector.
   */
  void reschedule(GlobalSchedGroupInfo groupInfo);
}