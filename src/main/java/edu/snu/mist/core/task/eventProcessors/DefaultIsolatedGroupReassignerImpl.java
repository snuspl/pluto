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

import edu.snu.mist.core.task.eventProcessors.groupAssigner.GroupAssigner;
import edu.snu.mist.core.task.eventProcessors.parameters.UnderloadedGroupThreshold;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * Remove threads that run isolated groups if the load of isolated groups is small.  
 */
public final class DefaultIsolatedGroupReassignerImpl implements IsolatedGroupReassigner {
  private static final Logger LOG = Logger.getLogger(DefaultIsolatedGroupReassignerImpl.class.getName());

  private final GroupAllocationTable groupAllocationTable;
  private final double underloadedGroupThreshold;
  private final GroupAssigner groupAssigner;

  @Inject
  private DefaultIsolatedGroupReassignerImpl(
      final GroupAllocationTable groupAllocationTable,
      @Parameter(UnderloadedGroupThreshold.class) final double underloadedGroupThreshold,
      final GroupAssigner groupAssigner) {
    this.groupAllocationTable = groupAllocationTable;
    this.underloadedGroupThreshold = underloadedGroupThreshold;
    this.groupAssigner = groupAssigner;
  }

  @Override
  public void reassignIsolatedGroups() {
    /* Re-implement this method
    final Iterator<EventProcessor> iterator = groupAllocationTable.getKeys().iterator();
    while (iterator.hasNext()) {
      final EventProcessor eventProcessor = iterator.next();
      if (eventProcessor.isRunningIsolatedGroup()) {
        final RuntimeProcessingInfo runtimeInfo = eventProcessor.getCurrentRuntimeInfo();
        final SubGroup group = runtimeInfo.getCurrGroup();
        if (group.getLoad() <= underloadedGroupThreshold) {
          LOG.log(Level.INFO, "Removing a thread for isolation: {0}",
              new Object[] {group});

          groupAllocationTable.remove(eventProcessor);
          try {
            eventProcessor.close();
          } catch (final Exception e) {
            e.printStackTrace();
          }
          // Re-assign this group
          groupAssigner.assignGroup(group);
        }
      }
    }
    */
  }
}