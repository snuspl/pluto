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
import javax.inject.Inject;

/**
 * This is a default implementation for globally shared next group selector.
 * It is created for the execution models that do not use a globally shared next group selector.
 */
public final class DefaultSharedNextGroupSelector implements NextGroupSelector {

  @Inject
  private DefaultSharedNextGroupSelector() {
    // do nothing
  }

  @Override
  public void onNext(final GroupEvent groupEvent) {

  }

  @Override
  public GlobalSchedGroupInfo getNextExecutableGroup() {
    return null;
  }

  @Override
  public void reschedule(final GlobalSchedGroupInfo groupInfo, final boolean miss) {

  }
}