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
package edu.snu.mist.core.task.groupaware.rebalancer;

import edu.snu.mist.core.task.Query;
import edu.snu.mist.core.task.groupaware.Group;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import edu.snu.mist.core.task.groupaware.GroupAllocationTable;
import edu.snu.mist.core.task.groupaware.parameters.DefaultGroupLoad;
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
    final long startTime = System.nanoTime();

    for (final Group group : groups) {
      double load = 0.0;

      final List<Query> queries = group.getQueries();
      final long processingEvent = group.getProcessingEvent().get();
      group.getProcessingEvent().addAndGet(-processingEvent);
      final long incomingEvent = processingEvent + group.numberOfRemainingEvents();
      final long processingEventTime = group.getProcessingTime().get();
      group.getProcessingTime().addAndGet(-processingEventTime);

      final long incomingEventTime = startTime - group.getLatestRebalanceTime();
      group.setLatestRebalanceTime(startTime);

      // Calculate group load
      // No processed. This thread is overloaded!
      // Just use the previous load
      if (processingEventTime == 0 && incomingEvent != 0) {
        //isOverloaded = true;
        load = group.getLoad();
      } else if (incomingEvent == 0) {
        // No incoming event
        load = defaultGroupLoad;
      } else {
        // processed event, incoming event
        final double inputRate = (incomingEvent * 1000000000) / (double) incomingEventTime;
        final double processingRate = (processingEvent * 1000000000) / (double) processingEventTime;
        final double groupLoad = inputRate / processingRate;
        load = groupLoad;
      }

      eventProcessorLoad += load;
      group.setLoad(load);

      // Calculate query load based on the group load!
      for (final Query query : queries) {
        // Number of processed events
        final long queryProcessingEvent = query.getProcessingEvent().get();
        query.getProcessingEvent().getAndAdd(-queryProcessingEvent);

        final long incomingE = query.numberOfRemainingEvents();

        final long queryIncomingEvent = incomingE + queryProcessingEvent;
        if (incomingEvent == 0) {
          query.setLoad(0);
        } else {
          query.setLoad(load * (queryIncomingEvent / (double) incomingEvent));
        }
      }
    }

    eventProcessor.setLoad(eventProcessorLoad);

    // Overloaded!
    /*
    if (isOverloaded) {
      eventProcessor.setLoad(1.0);
      // distribute the load
      final int size = groups.size();
      final double balancedLoad = 1.0 / size;

      for (final Group group : groups) {
        group.setLoad(balancedLoad);
        final int querySize = group.getQueries().size();
        final double queryLoad = balancedLoad / querySize;
        for (final Query query : group.getQueries()) {
          query.setLoad(queryLoad);
        }
      }
    } else {
      eventProcessor.setLoad(eventProcessorLoad);
    }
    */
  }
}