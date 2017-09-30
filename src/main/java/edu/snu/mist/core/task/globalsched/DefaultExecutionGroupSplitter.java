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

import edu.snu.mist.core.task.eventProcessors.EventProcessorManager;

import javax.inject.Inject;

public final class DefaultExecutionGroupSplitter implements ExecutionGroupSplitter {

  private final EventProcessorManager eventProcessorManager;

  @Inject
  private DefaultExecutionGroupSplitter(final EventProcessorManager eventProcessorManager) {
    this.eventProcessorManager = eventProcessorManager;
  }

  @Override
  public void splitGroup(final SubGroup group) {
    final Group superGroup = group.getGroup();
    // TODO
  }
}