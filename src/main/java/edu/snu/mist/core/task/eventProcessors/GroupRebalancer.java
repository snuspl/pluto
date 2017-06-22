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
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;


/**
 * GroupRebalancer reassigns the assigned groups from an event processor to other event processors.
 */
@DefaultImplementation(DefaultGroupRebalancerImpl.class)
public interface GroupRebalancer {

  /**
   * Reassign groups when the number of event processors increases.
   * @param newEpGroups new event processors
   * @param currEpGroups current event processors and the assigned groups
   */
  void reassignGroupsForNewEps(List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> newEpGroups,
                               List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> currEpGroups);


  /**
   * Reassign groups when the numbre of event processors decreases.
   * @param removedEps event processors that are removed
   * @param currEpGroups current event processors and the assigned groups
   */
  void reassignGroupsForRemovedEps(List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> removedEps,
                                   List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> currEpGroups);

  /**
   * Reassign groups for load rebalancing.
   * @param currEpGroups current event processors and the assigned groups.
   */
  void reassignGroups(List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> currEpGroups);
}
