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

/**
 * Default group rebalancer.
 */
public final class DefaultGroupRebalancerImpl implements GroupRebalancer {

  @Inject
  private DefaultGroupRebalancerImpl() {
  }


  @Override
  public void reassignGroupsForNewEps(final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> newEpGroups,
                                      final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> currEpGroups) {
    // TODO[MIST-799]: Reassign groups when increasing or decreasing event processors
  }

  @Override
  public void reassignGroupsForRemovedEps(final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> removedEps,
                                          final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> currEpGroups) {
    // TODO[MIST-799]: Reassign groups when increasing or decreasing event processors
  }

  @Override
  public void reassignGroups(final List<Tuple<EventProcessor,
      List<GlobalSchedGroupInfo>>> currEpGroups) {
    // TODO[MIST-799]: Reassign groups when increasing or decreasing event processors
  }
}
