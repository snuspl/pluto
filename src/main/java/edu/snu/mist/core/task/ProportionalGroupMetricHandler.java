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
package edu.snu.mist.core.task;

import edu.snu.mist.core.parameters.ThreadNumSoftLimit;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * This is a GroupMetricHandler assigns event processors to each group proportionally to it's metric.
 */
final class ProportionalGroupMetricHandler implements GroupMetricHandler {

  /**
   * The soft limit of the total number of executor threads.
   * If there are more groups than this number,
   * event processors according to the number of groups will be created ignoring this value.
   */
  final int threadNumSoftLimit;

  /**
   * The map of group ids and group info to update.
   */
  private final GroupInfoMap groupInfoMap;

  @Inject
  private ProportionalGroupMetricHandler(@Parameter(ThreadNumSoftLimit.class) final int threadNumSoftLimit,
                                         final GroupInfoMap groupInfoMap) {
    this.threadNumSoftLimit = threadNumSoftLimit;
    this.groupInfoMap = groupInfoMap;
  }

  /**
   * Assign single event processor number to each group.
   */
  private void assignSingleThread() {
    groupInfoMap.values().forEach(groupInfo -> groupInfo.getEventProcessorManager().adjustEventProcessorNum(1));
  }

  /**
   * Assign event processor number.
   * Every group does not have any event will have one event processor number.
   * Other groups will have the portion of remainder proportionally to it's metric.
   */
  @Override
  public void groupMetricUpdated() {
    if (groupInfoMap.size() >= threadNumSoftLimit) {
      // Every group should not totally blocked because of another group
      // Because of this, we assign at least one event processor number to each group
      assignSingleThread();
    } else {
      // Calculate the sum of the number of events
      long sum = 0;
      int zeroCount = 0;
      for (final GroupInfo groupInfo : groupInfoMap.values()) {
        final long numEvents = groupInfo.getGroupMetric().getNumEvents();
        if (numEvents == 0) {
          zeroCount++;
          continue;
        }
        sum += numEvents;
      }
      // The number of event processors which are assignable additionally
      final int remainderProcessorNum = threadNumSoftLimit - zeroCount;

      if (sum == 0) {
        // If sum of events is zero, than we should assign one event procesosr number to each group
        assignSingleThread();
      } else {
        for (final GroupInfo groupInfo : groupInfoMap.values()) {
          final long numEvents = groupInfo.getGroupMetric().getNumEvents();
          // Assign processor number proportionally to the number of events
          long processorNumToAssign = remainderProcessorNum * numEvents / sum;
          if (processorNumToAssign == 0) {
            // Each group needs at lease one event processor
            processorNumToAssign = 1;
          }
          groupInfo.getEventProcessorManager().adjustEventProcessorNum(processorNumToAssign);
        }
      }
    }
  }
}
