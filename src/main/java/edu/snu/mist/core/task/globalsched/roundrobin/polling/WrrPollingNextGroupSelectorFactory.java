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

import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfoMap;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import edu.snu.mist.core.task.globalsched.NextGroupSelectorFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * This creates new WeightedRRNextGroupSelector.
 */
public final class WrrPollingNextGroupSelectorFactory implements NextGroupSelectorFactory {

  /**
   * Mist pub/sub event handler.
   */
  private final MistPubSubEventHandler mistPubSubEventHandler;

  /**
   * Group info map.
   */
  private final GlobalSchedGroupInfoMap globalSchedGroupInfoMap;

  /**
   * Polling period.
   */
  private final long pollingPeriod;

  /**
   * Inactive group checker factory.
   */
  private final InactiveGroupCheckerFactory inactiveGroupCheckerFactory;

  @Inject
  private WrrPollingNextGroupSelectorFactory(final MistPubSubEventHandler mistPubSubEventHandler,
                                             final GlobalSchedGroupInfoMap globalSchedGroupInfoMap,
                                             @Parameter(PollingPeriod.class) final long pollingPeriod,
                                             final InactiveGroupCheckerFactory inactiveGroupCheckerFactory) {
    this.mistPubSubEventHandler = mistPubSubEventHandler;
    this.globalSchedGroupInfoMap = globalSchedGroupInfoMap;
    this.pollingPeriod = pollingPeriod;
    this.inactiveGroupCheckerFactory = inactiveGroupCheckerFactory;
  }

  @Override
  public NextGroupSelector newInstance() {
    return new WrrPollingNextGroupSelector(
        mistPubSubEventHandler, globalSchedGroupInfoMap, inactiveGroupCheckerFactory.newInstance(), pollingPeriod);
  }
}