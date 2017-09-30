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

import edu.snu.mist.core.task.globalsched.Group;
import edu.snu.mist.core.task.globalsched.SubGroup;
import edu.snu.mist.core.task.globalsched.parameters.DefaultGroupLoad;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

public final class UtilizationLoadUpdater implements LoadUpdater {
  private static final Logger LOG = Logger.getLogger(UtilizationLoadUpdater.class.getName());

  /**
   * Default group load.
   */
  private final double defaultGroupLoad;

  /**
   * Group allocation table.
   */
  private final GroupAllocationTable groupAllocationTable;

  @Inject
  private UtilizationLoadUpdater(final GroupAllocationTable groupAllocationTable,
                                 @Parameter(DefaultGroupLoad.class) final double defaultGroupLoad) {
    this.defaultGroupLoad = defaultGroupLoad;
    this.groupAllocationTable = groupAllocationTable;
  }

  @Override
  public void update() {
    final List<EventProcessor> eventProcessors = groupAllocationTable.getKeys();
    for (final EventProcessor eventProcessor : eventProcessors) {
      updateGroupAndThreadLoad(eventProcessor, groupAllocationTable.getValue(eventProcessor));
    }
    LOG.info(groupAllocationTable.toString());
  }


  /**
   * Update the load of the event processor and the assigned groups.
   * @param eventProcessor event processor
   * @param groups assigned groups
   */
  private void updateGroupAndThreadLoad(final EventProcessor eventProcessor,
                                        final Collection<Group> groups) {
    //boolean isOverloaded = false;

    double eventProcessorLoad = 0.0;

    for (final Group group : groups) {
      double load = 0.0;

      final List<SubGroup> subGroupList = group.getSubGroups();
      synchronized (subGroupList) {
        for (final SubGroup subGroup : subGroupList) {
          final long startTime = System.nanoTime();

          // Number of processed events
          final long processingEvent = subGroup.getProcessingEvent().get();
          subGroup.getProcessingEvent().getAndAdd(-processingEvent);

          // Number of incoming events
          final long incomingEventTime = startTime - subGroup.getLatestRebalanceTime();
          subGroup.setLatestRebalanceTime(startTime);

          final long incomingE = subGroup.numberOfRemainingEvents();
          final long incomingEvent = incomingE + processingEvent;

          final long processingEventTime = subGroup.getProcessingTime().get();
          subGroup.getProcessingTime().getAndAdd(-processingEventTime);

          // No processed. This thread is overloaded!
          if (processingEventTime == 0 && incomingEvent != 0) {
            //isOverloaded = true;
          } else if (incomingEvent == 0) {
            // No incoming event
            subGroup.setLoad(defaultGroupLoad);
            load += defaultGroupLoad;
          } else {
            // processed event, incoming event
            final double inputRate = (incomingEvent * 1000000000) / (double) incomingEventTime;
            final double processingRate = (processingEvent * 1000000000) / (double) processingEventTime;
            final double groupLoad = inputRate / processingRate;
            load += groupLoad;

            subGroup.setLoad(groupLoad);
          }
        }
      }
      group.setLoad(load);
      eventProcessorLoad += load;
    }


    // Overloaded!
    /*
    if (isOverloaded) {
      eventProcessor.setLoad(1.0);
      // distribute the load
      final int size = groups.size();
      final double balancedLoad = 1.0 / size;
      for (final SubGroup group : groups) {
        group.setLoad(balancedLoad);
      }
    } else {
      eventProcessor.setLoad(load);
    }
    */

    eventProcessor.setLoad(eventProcessorLoad);
  }
}