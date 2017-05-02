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
package edu.snu.mist.core.task.globalsched.cfs;
import edu.snu.mist.core.parameters.DefaultGroupWeight;
import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import edu.snu.mist.core.task.globalsched.NextGroupSelectorFactory;
import edu.snu.mist.core.task.globalsched.cfs.parameters.MinSchedulingPeriod;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * This creates new VtimeBasedNextGroupSelector.
 */
public final class VtimeBasedNextGroupSelectorFactory implements NextGroupSelectorFactory {

  /**
   * Mist pub/sub event handler.
   */
  private final MistPubSubEventHandler mistPubSubEventHandler;

  /**
   * Default weight of the group.
   */
  private final double defaultWeight;

  /**
   * The minimum scheduling period per group.
   */
  private final long minSchedPeriod;

  @Inject
  private VtimeBasedNextGroupSelectorFactory(@Parameter(DefaultGroupWeight.class) final double defaultWeight,
                                             @Parameter(MinSchedulingPeriod.class) final long minSchedPeriod,
                                             final MistPubSubEventHandler mistPubSubEventHandler) {
    this.defaultWeight = defaultWeight;
    this.minSchedPeriod = minSchedPeriod;
    this.mistPubSubEventHandler = mistPubSubEventHandler;
  }

  @Override
  public NextGroupSelector newInstance() {
    return new VtimeBasedNextGroupSelector(defaultWeight, minSchedPeriod, mistPubSubEventHandler);
  }
}