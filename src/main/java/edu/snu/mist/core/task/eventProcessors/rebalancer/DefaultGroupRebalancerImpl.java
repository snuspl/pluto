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
package edu.snu.mist.core.task.eventProcessors.rebalancer;

import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.eventProcessors.GroupAllocationTable;
import edu.snu.mist.core.task.eventProcessors.parameters.*;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.parameters.DefaultGroupLoad;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This rebalancer reassignments groups from overloaded threads to underloaded threads.
 * If beta > load(threads), it considers the threads overloaded
 * If alpha < load(threads), it considers the threads underloaded
 *
 * After finding the overloaded and underloaded threads, it moves the groups
 * assiged to the overloaded threads to the underloaded threads.
 */
public final class DefaultGroupRebalancerImpl implements GroupRebalancer {
  private static final Logger LOG = Logger.getLogger(DefaultGroupRebalancerImpl.class.getName());

  /**
   * Group allocation table.
   */
  private final GroupAllocationTable groupAllocationTable;

  /**
   * Rebalancing period.
   */
  private final long rebalancingPeriod;

  /**
   * Default group load.
   */
  private final double defaultGroupLoad;

  /**
   * Threshold for overloaded threads.
   */
  private final double beta;

  /**
   * Threshold for underloaded threads.
   */
  private final double alpha;

  @Inject
  private DefaultGroupRebalancerImpl(final GroupAllocationTable groupAllocationTable,
                                     @Parameter(GroupRebalancingPeriod.class) final long rebalancingPeriod,
                                     @Parameter(DefaultGroupLoad.class) final double defaultGroupLoad,
                                     @Parameter(OverloadedThreshold.class) final double beta,
                                     @Parameter(UnderloadedThreshold.class) final double alpha) {
    this.groupAllocationTable = groupAllocationTable;
    this.rebalancingPeriod = rebalancingPeriod;
    this.defaultGroupLoad = defaultGroupLoad;
    this.alpha = alpha;
    this.beta = beta;
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

  private void logging(final List<EventProcessor> eventProcessors,
                       final Map<EventProcessor, Double> loadTable) {
    final StringBuilder sb = new StringBuilder();
    sb.append("-------------- TABLE ----------------\n");
    for (final EventProcessor ep : eventProcessors) {
      final Collection<GlobalSchedGroupInfo> groups = groupAllocationTable.getValue(ep);
      sb.append(ep);
      sb.append(" -> [");
      sb.append(loadTable.get(ep));
      sb.append("], [");
      sb.append(groups.size());
      sb.append("], ");
      sb.append(groups);
      sb.append("\n");
    }
    LOG.info(sb.toString());
  }

  @Override
  public void triggerRebalancing() {
    LOG.info("REBALANCING START");
    long rebalanceStart = System.currentTimeMillis();

    try {
      final List<EventProcessor> eventProcessors = groupAllocationTable.getKeys();
      // Overloaded threads
      final List<EventProcessor> overloadedThreads = new LinkedList<>();

      // Underloaded threads
      final PriorityQueue<Tuple<EventProcessor, Double>> underloadedThreads =
          new PriorityQueue<>(new Comparator<Tuple<EventProcessor, Double>>() {
            @Override
            public int compare(final Tuple<EventProcessor, Double> o1, final Tuple<EventProcessor, Double> o2) {
              final Double load1 = o1.getValue();
              final Double load2 = o2.getValue();
              return load1.compareTo(load2);
            }
          });

      // Calculate each load and total load
      final Map<EventProcessor, Double> loadTable = new HashMap<>();

      for (final EventProcessor eventProcessor : eventProcessors) {
        final double load = eventProcessor.getLoad();
        loadTable.put(eventProcessor, load);
        if (load > beta) {
            overloadedThreads.add(eventProcessor);
        } else if (load < alpha) {
            underloadedThreads.add(new Tuple<>(eventProcessor, load));
        }
      }

      // LOGGING
      //logging(eventProcessors, loadTable);

      double targetLoad = (alpha + beta) / 2;

      int rebNum = 0;

      if (!overloadedThreads.isEmpty() && !underloadedThreads.isEmpty()) {
        for (final EventProcessor highLoadThread : overloadedThreads) {
          final Collection<GlobalSchedGroupInfo> highLoadGroups = groupAllocationTable.getValue(highLoadThread);
          final Iterator<GlobalSchedGroupInfo> iterator = highLoadGroups.iterator();
          double highLoad = loadTable.get(highLoadThread);

          while (iterator.hasNext()) {
            final GlobalSchedGroupInfo selectedGroup = iterator.next();
            final double groupLoad = selectedGroup.getLoad();

            if (highLoad - groupLoad >= targetLoad) {
              final Tuple<EventProcessor, Double> peek = underloadedThreads.peek();
              if (peek.getValue() + groupLoad <= targetLoad) {
                if (selectedGroup.isReady() || highLoadThread.removeActiveGroup(selectedGroup)) {
                  final Tuple<EventProcessor, Double> lowLoadThread = underloadedThreads.poll();
                  final Collection<GlobalSchedGroupInfo> lowLoadGroups =
                      groupAllocationTable.getValue(lowLoadThread.getKey());

                  lowLoadGroups.add(selectedGroup);
                  iterator.remove();

                  // Update overloaded thread load
                  highLoad -= groupLoad;
                  highLoadThread.setLoad(highLoad - groupLoad);
                  rebNum += 1;

                  // Update underloaded thread load
                  lowLoadThread.getKey().setLoad(lowLoadThread.getValue() + groupLoad);
                  underloadedThreads.add(new Tuple<>(lowLoadThread.getKey(), lowLoadThread.getValue() + groupLoad));
                }
              }
            }
          }
        }
      }

      long rebalanceEnd = System.currentTimeMillis();
      LOG.log(Level.INFO, "Rebalancing number: {0}, elapsed time: {1}",
          new Object[]{rebNum, rebalanceEnd - rebalanceStart});

      //LOG.log(Level.INFO, "-------------TABLE-------------\n{0}",
      //new Object[]{groupAllocationTable.toString()});
    } catch (final Exception e) {
      LOG.log(Level.WARNING, "Exception " + e);
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}