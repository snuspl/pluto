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

import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.task.Query;
import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.eventProcessors.GroupAllocationTable;
import edu.snu.mist.core.task.eventProcessors.parameters.GroupRebalancingPeriod;
import edu.snu.mist.core.task.eventProcessors.parameters.OverloadedThreshold;
import edu.snu.mist.core.task.eventProcessors.parameters.UnderloadedThreshold;
import edu.snu.mist.core.task.globalsched.Group;
import edu.snu.mist.core.task.globalsched.parameters.DefaultGroupLoad;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
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
public final class DefaultGroupSplitterImpl implements GroupSplitter {
  private static final Logger LOG = Logger.getLogger(DefaultGroupSplitterImpl.class.getName());

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

  private final double epsilon = 0.00000001;

  @Inject
  private DefaultGroupSplitterImpl(final GroupAllocationTable groupAllocationTable,
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
      final Collection<Group> groups = groupAllocationTable.getValue(ep);
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

  private Group hasSameGroup(final Group group, final EventProcessor eventProcessor) {
    for (final Group g : groupAllocationTable.getValue(eventProcessor)) {
      if (g.getGroupId().equals(group.getGroupId())) {
        return g;
      }
    }
    return null;
  }

  @Override
  public void splitGroup() {
    LOG.info("GROUP SPLIT START");
    long rebalanceStart = System.currentTimeMillis();

    try {
      // Skip if it is an isolated processor that runs an isolated group
      final List<EventProcessor> eventProcessors = groupAllocationTable.getEventProcessorsNotRunningIsolatedGroup();
      // Overloaded threads
      final List<EventProcessor> overloadedThreads = new LinkedList<>();

      // Underloaded threads
      final PriorityQueue<EventProcessor> underloadedThreads =
          new PriorityQueue<>(new Comparator<EventProcessor>() {
            @Override
            public int compare(final EventProcessor o1, final EventProcessor o2) {
              final Double load1 = o1.getLoad();
              final Double load2 = o2.getLoad();
              return load1.compareTo(load2);
            }
          });

      // Calculate each load and total load
      for (final EventProcessor eventProcessor : eventProcessors) {
        final double load = eventProcessor.getLoad();
        if (load > beta) {
          overloadedThreads.add(eventProcessor);
        } else if (load < alpha) {
          underloadedThreads.add(eventProcessor);
        }
      }

      // LOGGING
      //logging(eventProcessors, loadTable);

      double targetLoad = (alpha + beta) / 2;

      int rebNum = 0;

      Collections.sort(overloadedThreads, new Comparator<EventProcessor>() {
        @Override
        public int compare(final EventProcessor o1, final EventProcessor o2) {
          return o1.getLoad() < o2.getLoad() ? 1 : -1;
        }
      });

      if (!overloadedThreads.isEmpty() && !underloadedThreads.isEmpty()) {
        for (final EventProcessor highLoadThread : overloadedThreads) {
          final Collection<Group> highLoadGroups = groupAllocationTable.getValue(highLoadThread);
          final List<Group> sortedHighLoadGroups = new LinkedList<>(highLoadGroups);

          Collections.sort(sortedHighLoadGroups, new Comparator<Group>() {
            @Override
            public int compare(final Group o1, final Group o2) {
              if (o1.isSplited() && !o2.isSplited()) {
                return -1;
              } else if (!o1.isSplited() && o2.isSplited()) {
                return 1;
              } else {
                if (o1.getLoad() < o2.getLoad()) {
                  return -1;
                } else {
                  return 1;
                }
              }
            }
          });

          for (final Group highLoadGroup : sortedHighLoadGroups) {
            // Split if the load of the high load thread could be less than targetLoad
            // when we split the high load group
            if (highLoadThread.getLoad() - highLoadGroup.getLoad() < targetLoad + epsilon
                && highLoadGroup.size() > 1) {

              // Sorting queries
              final List<Query> queries = highLoadGroup.getQueries();
              final List<Query> sortedQueries = new ArrayList<>(queries);
              sortedQueries.sort(new Comparator<Query>() {
                @Override
                public int compare(final Query o1, final Query o2) {
                  return o1.getLoad() < o2.getLoad() ? 1 : -1;
                }
              });

              final EventProcessor lowLoadThread = underloadedThreads.poll();
              Group sameGroup = hasSameGroup(highLoadGroup, lowLoadThread);

              for (final Query movingQuery : sortedQueries) {
                if (highLoadThread.getLoad() - movingQuery.getLoad() >= targetLoad - epsilon &&
                    lowLoadThread.getLoad() + movingQuery.getLoad() <= targetLoad + epsilon) {

                  if (sameGroup == null) {
                    // Split! Create a new group!
                    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
                    jcb.bindNamedParameter(GroupId.class, highLoadGroup.getGroupId());
                    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
                    sameGroup = injector.getInstance(Group.class);
                    sameGroup.setEventProcessor(lowLoadThread);
                    highLoadGroup.getMetaGroup().addGroup(sameGroup);
                    groupAllocationTable.getValue(lowLoadThread).add(sameGroup);
                  }

                  // Move to the existing group!
                  sameGroup.addQuery(movingQuery);
                  sameGroup.setLoad(sameGroup.getLoad() + movingQuery.getLoad());

                  queries.remove(movingQuery);
                  highLoadGroup.setLoad(highLoadGroup.getLoad() - movingQuery.getLoad());

                  lowLoadThread.setLoad(lowLoadThread.getLoad() + movingQuery.getLoad());
                  highLoadThread.setLoad(highLoadThread.getLoad() - movingQuery.getLoad());

                  rebNum += 1;
                }
              }

              underloadedThreads.add(lowLoadThread);
            }
          }
        }
      }

      long rebalanceEnd = System.currentTimeMillis();
      LOG.log(Level.INFO, "GroupSplit number: {0}, elapsed time: {1}",
          new Object[]{rebNum, rebalanceEnd - rebalanceStart});

      //LOG.log(Level.INFO, "-------------TABLE-------------\n{0}",
      //new Object[]{groupAllocationTable.toString()});
    } catch (final Exception e) {
      LOG.log(Level.WARNING, "Exception " + e);
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /*
  @Override
  public void splitGroup() {
    LOG.info("SPLITTING START");
    long rebalanceStart = System.currentTimeMillis();

    try {
      // Skip if it is an isolated processor that runs an isolated group
      final List<EventProcessor> eventProcessors = groupAllocationTable.getEventProcessorsNotRunningIsolatedGroup();
      // Overloaded threads
      final List<EventProcessor> overloadedThreads = new LinkedList<>();

      // Underloaded threads
      final PriorityQueue<EventProcessor> underloadedThreads =
          new PriorityQueue<>(new Comparator<EventProcessor>() {
            @Override
            public int compare(final EventProcessor o1, final EventProcessor o2) {
              final Double load1 = o1.getLoad();
              final Double load2 = o2.getLoad();
              return load1.compareTo(load2);
            }
          });

      // Calculate each load and total load
      for (final EventProcessor eventProcessor : eventProcessors) {
        final double load = eventProcessor.getLoad();
        if (load > beta) {
          overloadedThreads.add(eventProcessor);
        } else if (load < alpha) {
          underloadedThreads.add(eventProcessor);
        }
      }

      // LOGGING
      //logging(eventProcessors, loadTable);

      double targetLoad = (alpha + beta) / 2;

      int rebNum = 0;

      if (!overloadedThreads.isEmpty() && !underloadedThreads.isEmpty()) {
        for (final EventProcessor highLoadThread : overloadedThreads) {
          // We  need to split because the load of the thread is still high
          final Collection<Group> highLoadGroups = groupAllocationTable.getValue(highLoadThread);
          final List<Group> sortedHighLoadGroups = new LinkedList<>(highLoadGroups);

          Collections.sort(sortedHighLoadGroups, new Comparator<Group>() {
            @Override
            public int compare(final Group o1, final Group o2) {
              if (o1.isSplited() && !o2.isSplited()) {
                return -1;
              } else if (!o1.isSplited() && o2.isSplited()) {
                return 1;
              } else {
                if (o1.getLoad() < o2.getLoad()) {
                  return -1;
                } else {
                  return 1;
                }
              }
            }
          });

          for (final Group highLoadGroup : sortedHighLoadGroups) {
            // Split group!

            if (highLoadGroup.isSplited()) {
              // Consider splitted group first
              final PriorityQueue<Group> lowLoadThreadsGroups = threadsInSplittedGroup(highLoadGroup);

              // Merge
              while (!lowLoadThreadsGroups.isEmpty()) {
                final Group lowLoadGroup = lowLoadThreadsGroups.poll();
                synchronized (highLoadGroup.getSubGroups()) {
                  final Iterator<SubGroup> subGroupIterator = highLoadGroup.getSubGroups().iterator();
                  while (subGroupIterator.hasNext()) {
                    final SubGroup subGroup = subGroupIterator.next();
                    if (lowLoadGroup.getEventProcessor().getLoad() + subGroup.getLoad() <= targetLoad &&
                        highLoadThread.getLoad() - subGroup.getLoad() >= targetLoad) {
                      lowLoadGroup.addSubGroup(subGroup);
                      subGroupIterator.remove();

                      highLoadThread.setLoad(highLoadThread.getLoad() - subGroup.getLoad());
                      lowLoadGroup.getEventProcessor()
                          .setLoad(lowLoadGroup.getEventProcessor().getLoad() + subGroup.getLoad());
                    }
                  }
                }
              }
            }
          }
        }
      }

      long rebalanceEnd = System.currentTimeMillis();
      LOG.log(Level.INFO, "Split merge number: {0}, elapsed time: {1}",
          new Object[]{rebNum, rebalanceEnd - rebalanceStart});

      //LOG.log(Level.INFO, "-------------TABLE-------------\n{0}",
      //new Object[]{groupAllocationTable.toString()});
    } catch (final Exception e) {
      LOG.log(Level.WARNING, "Exception " + e);
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  */
}