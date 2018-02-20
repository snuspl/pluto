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

import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import edu.snu.mist.core.task.groupaware.GroupAllocationTable;
import edu.snu.mist.core.task.groupaware.Group;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is the first-fit group balancer.
 */
@Deprecated
public final class FirstFitRebalancerImpl implements GroupRebalancer {
  private static final Logger LOG = Logger.getLogger(FirstFitRebalancerImpl.class.getName());

  private final GroupAllocationTable groupAllocationTable;

  @Inject
  private FirstFitRebalancerImpl(final GroupAllocationTable groupAllocationTable) {
    this.groupAllocationTable = groupAllocationTable;
  }

  /**
   * This is for logging.
   * @param loadTable loadTable
   */
  private String printMap(final Map<EventProcessor, Double> loadTable) {
    final StringBuilder sb = new StringBuilder();
    for (final Map.Entry<EventProcessor, Double> entry : loadTable.entrySet()) {
      sb.append(entry.getKey());
      sb.append(" -> ");
      sb.append(entry.getValue());
      sb.append("\n");
    }
    return sb.toString();
  }

  /**
   * Calculate the load of groups.
   * @param groups groups
   * @return total load
   */
  private double calculateLoadOfGroupsForLogging(final Collection<Group> groups) {
    double sum = 0;
    for (final Group group : groups) {
      final double fixedLoad = group.getLoad();
      sum += fixedLoad;
    }
    return sum;
  }

  @Override
  public void triggerRebalancing() {
    final List<EventProcessor> eventProcessors = groupAllocationTable.getEventProcessorsNotRunningIsolatedGroup();
    // Calculate each load and total load
    final Map<EventProcessor, Double> loadTable = new HashMap<>();
    double totalLoad = 0.0;
    for (final EventProcessor eventProcessor : eventProcessors) {
      final double load = eventProcessor.getLoad();
      totalLoad += load;
      eventProcessor.setLoad(load);
      loadTable.put(eventProcessor, load);
    }

    // Desirable load
    // Each event processor should has the load evenly
    final double desirableLoad = totalLoad * (1.0 / eventProcessors.size());

    // Make bins and items.
    final Tuple<Map<EventProcessor, Double>, List<Group>> binsAndItems =
        makeBinsAndItems(desirableLoad, loadTable, eventProcessors);

    // First-fit heuristic algorithm
    final Map<Group, EventProcessor> mapping = firstFitHeuristic(
        eventProcessors, binsAndItems.getKey(), binsAndItems.getValue());

    // Reassign the groups to the event processor
    for (final EventProcessor eventProcessor : eventProcessors) {
      final Iterator<Group> iterator = groupAllocationTable.getValue(eventProcessor).iterator();
      while (iterator.hasNext()) {
        final Group group = iterator.next();
        final EventProcessor destEP = mapping.remove(group);
        if (destEP != null) {
          iterator.remove();
          final Collection<Group> dest = groupAllocationTable.getValue(destEP);
          dest.add(group);
        }
      }
    }

    if (LOG.isLoggable(Level.FINE)) {
      final Map<EventProcessor, Double> afterRebalancingLoadTable = new HashMap<>();
      for (final EventProcessor ep : groupAllocationTable.getKeys()) {
        afterRebalancingLoadTable.put(ep, calculateLoadOfGroupsForLogging(groupAllocationTable.getValue(ep)));
      }

      LOG.log(Level.FINE, "Rebalanacing Groups (Desirable load={0}) \n"
          + "=========== Before LB ==========\n {1} \n"
          + "=========== After  LB ==========\n {2} \n",
          new Object[] {desirableLoad, printMap(loadTable), printMap(afterRebalancingLoadTable)});
    }
  }

  private Map<Group, EventProcessor> firstFitHeuristic(
      final List<EventProcessor> eventProcessors,
      final Map<EventProcessor, Double> bins,
      final List<Group> items) {
    final Map<Group, EventProcessor> mapping = new HashMap<>(items.size());

    final Iterator<Group> iterator = items.iterator();
    while (iterator.hasNext()) {
      final Group item = iterator.next();
      // find the first bin that can hold the item
      for (final EventProcessor eventProcessor : eventProcessors) {
        final double size = bins.get(eventProcessor);
        final double itemSize = item.getLoad();
        if (size >= itemSize) {
          // This is the first bin that can hold the item!
          iterator.remove();
          mapping.put(item, eventProcessor);
          bins.put(eventProcessor, size - itemSize);
          break;
        }
      }
    }

    if (!items.isEmpty()) {
      // Second
      final Iterator<Group> secondIter = items.iterator();
      while (secondIter.hasNext()) {
        final Group item = secondIter.next();
        // find the first bin that can hold the item
        for (final EventProcessor eventProcessor : eventProcessors) {
          final double size = bins.get(eventProcessor);
          final double itemSize = item.getLoad();
          if (size > 0) {
            // This is the first bin that can hold the item!
            secondIter.remove();
            mapping.put(item, eventProcessor);
            bins.put(eventProcessor, size - itemSize);
            break;
          }
        }
      }
    }

    if (!items.isEmpty()) {
      throw new RuntimeException("First-fit algorithm is incorrect");
    }

    return mapping;
  }

  /**
   * Makes bins (event processors with the available size) and items (groups).
   * @param desirableLoad desirable load (size)
   * @param loadTable load table
   * @param eventProcessors event processors
   * @return bins and items
   */
  private Tuple<Map<EventProcessor, Double>, List<Group>> makeBinsAndItems(
      final double desirableLoad,
      final Map<EventProcessor, Double> loadTable,
      final List<EventProcessor> eventProcessors) {
    // Make bins
    final Map<EventProcessor, Double> bins = new HashMap<>(groupAllocationTable.size());

    // Make items
    final List<Group> items = new LinkedList<>();
    for (final EventProcessor eventProcessor : eventProcessors) {
      double load = loadTable.get(eventProcessor);
      if (load > desirableLoad) {
        // Add groups until the load is less than the desirable load
        final Iterator<Group> iterator = groupAllocationTable.getValue(eventProcessor).iterator();
        while (load > desirableLoad && iterator.hasNext()) {
          final Group group = iterator.next();
          items.add(group);
          load -= group.getLoad();
        }
      }

      // Put the bin with the size
      bins.put(eventProcessor, desirableLoad - load);
    }

    return new Tuple<>(bins, items);
  }
}
