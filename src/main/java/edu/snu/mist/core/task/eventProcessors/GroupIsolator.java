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

import org.apache.reef.tang.annotations.DefaultImplementation;


/**
 * This class isolates overloaded groups to isolated threads.
 * We can prevent the event processing of other groups from being delayed by the overloaded groups.
 */
@DefaultImplementation(DefaultGroupIsolatorImpl.class)
public interface GroupIsolator {

  /**
   * Trigger the isolation of groups.
   */
  void triggerIsolation();
}