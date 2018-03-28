/*
 * Copyright (C) 2018 Seoul National University
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

import edu.snu.mist.core.task.DefaultGroupIdRequestorImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * The interface for group id requestors which submit requests for group id.
 */
@DefaultImplementation(DefaultGroupIdRequestorImpl.class)
public interface GroupIdRequestor {

  /**
   * Request group id.
   * @param appId application id
   * @return the allocated group id
   */
  String requestGroupId(final String appId);
}
