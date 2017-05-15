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
package edu.snu.mist.core.task.globalsched.roundrobin.polling;

import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;

/**
 * This interface checks whether the group becomes inactive or not.
 */
public interface InactiveGroupChecker {

  /**
   * Checks whether the group becomes inactive.
   * @param groupInfo group info
   * @param miss true if the event processor cannot find an active operator chain in the group
   * @return true if it determines the group becomes inactive
   */
  boolean check(GlobalSchedGroupInfo groupInfo, boolean miss);
}
