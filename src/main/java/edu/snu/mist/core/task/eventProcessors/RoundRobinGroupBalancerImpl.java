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

import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round-robin group balancer that assigns the new group in a round-robin way.
 */
public final class RoundRobinGroupBalancerImpl implements GroupBalancer {

  /**
   * Counter for round-robin. 
   */
  private final AtomicLong counter;

  @Inject
  private RoundRobinGroupBalancerImpl() {
    this.counter = new AtomicLong(0);
  }

  @Override
  public void assignGroup(final GlobalSchedGroupInfo groupInfo,
                          final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> currEpGroups) {
    final int index = (int)(counter.getAndIncrement() % currEpGroups.size());
    final Tuple<EventProcessor, List<GlobalSchedGroupInfo>> tuple = currEpGroups.get(index);
    tuple.getValue().add(groupInfo);
  }
}
