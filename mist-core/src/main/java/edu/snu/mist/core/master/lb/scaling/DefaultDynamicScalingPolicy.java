/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.core.master.lb.scaling;

import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.lb.parameters.IdleTaskLoadThreshold;
import edu.snu.mist.core.master.lb.parameters.MaxTaskNum;
import edu.snu.mist.core.master.lb.parameters.MinTaskNum;
import edu.snu.mist.core.master.lb.parameters.OverloadedTaskLoadThreshold;
import edu.snu.mist.core.master.lb.parameters.ScaleInGracePeriod;
import edu.snu.mist.core.master.lb.parameters.ScaleInIdleTaskRate;
import edu.snu.mist.core.master.lb.parameters.ScaleOutGracePeriod;
import edu.snu.mist.core.master.lb.parameters.ScaleOutOverloadedTaskRate;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;

/**
 * The default implementation for dynamic scaling policy.
 */
public final class DefaultDynamicScalingPolicy implements DynamicScalingPolicy {

  /**
   * The shared stats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The maximum number of allocatable task.
   */
  private final int maxTaskNum;

  /**
   * The minimum number of allocatable task.
   */
  private final int minTaskNum;

  /**
   * The load threshold for determining idle task.
   */
  private final double idleTaskLoadThreshold;

  /**
   * The load threshold for determining overloaded task.
   */
  private final double overloadedTaskLoadThreshold;

  /**
   * The waiting time for scaling-in.
   */
  private final long scaleInGracePeriod;

  /**
   * The waiting time for scaling-out.
   */
  private final long scaleOutGracePeriod;

  /**
   * The rate of idle tasks for determining scaling-in action.
   */
  private final double scaleInIdleTaskRate;

  /**
   * The rate of overloaded tasks for determining scaling-out action.
   */
  private final double scaleOutOverloadedTaskRate;

  /**
   * The elapsed idle time.
   */
  private long idleTimeElapsed;

  /**
   * The elapsed overloaded time.
   */
  private long overloadedTimeElapsed;

  /**
   * The last measured timestamp.
   */
  private long lastMeasuredTimestamp;

  @Inject
  private DefaultDynamicScalingPolicy(
      final TaskStatsMap taskStatsMap,
      @Parameter(MaxTaskNum.class) final int maxTaskNum,
      @Parameter(MinTaskNum.class) final int minTaskNum,
      @Parameter(IdleTaskLoadThreshold.class) final double idleTaskLoadThreshold,
      @Parameter(OverloadedTaskLoadThreshold.class) final double overloadedTaskLoadThreshold,
      @Parameter(ScaleInGracePeriod.class) final long scaleInGracePeriod,
      @Parameter(ScaleOutGracePeriod.class) final long scaleOutGracePeriod,
      @Parameter(ScaleInIdleTaskRate.class) final double scaleInIdleTaskRate,
      @Parameter(ScaleOutOverloadedTaskRate.class) final double scaleOutOverloadedTaskRate
  ) {
    this.taskStatsMap = taskStatsMap;
    this.maxTaskNum = maxTaskNum;
    this.minTaskNum = minTaskNum;
    this.idleTaskLoadThreshold = idleTaskLoadThreshold;
    this.overloadedTaskLoadThreshold = overloadedTaskLoadThreshold;
    this.scaleInGracePeriod = scaleInGracePeriod;
    this.scaleOutGracePeriod = scaleOutGracePeriod;
    this.scaleInIdleTaskRate = scaleInIdleTaskRate;
    this.scaleOutOverloadedTaskRate = scaleOutOverloadedTaskRate;
    this.idleTimeElapsed = 0L;
    this.overloadedTimeElapsed = 0L;
    this.lastMeasuredTimestamp = 0L;
  }

  private boolean isClusterOverloaded() {
    int overloadedTaskNum = 0;
    for (final Map.Entry<String, TaskStats> entry : taskStatsMap.entrySet()) {
      if (entry.getValue().getTaskLoad() > overloadedTaskLoadThreshold) {
        overloadedTaskNum += 1;
      }
    }
    final double overloadedTaskRate = overloadedTaskNum / taskStatsMap.getTaskList().size();
    return overloadedTaskRate > scaleOutOverloadedTaskRate;
  }

  private boolean isClusterIdle() {
    int idleTaskNum = 0;
    for (final Map.Entry<String, TaskStats> entry : taskStatsMap.entrySet()) {
      if (entry.getValue().getTaskLoad() <= idleTaskLoadThreshold) {
        idleTaskNum += 1;
      }
    }
    final double idleTaskRate = idleTaskNum / taskStatsMap.getTaskList().size();
    return idleTaskRate > scaleOutOverloadedTaskRate;
  }

  @Override
  public ScalingAction getScalingAction() {
    final long oldTimeStamp;
    if (lastMeasuredTimestamp == 0L) {
      oldTimeStamp = System.currentTimeMillis();
      lastMeasuredTimestamp = oldTimeStamp;
    } else {
      oldTimeStamp = lastMeasuredTimestamp;
      lastMeasuredTimestamp = System.currentTimeMillis();
    }

    final boolean clusterOverloaded = isClusterOverloaded();
    final boolean clusterIdle = isClusterIdle();

    // Add to the
    if (clusterOverloaded) {
      this.overloadedTimeElapsed += lastMeasuredTimestamp - oldTimeStamp;
    } else {
      this.overloadedTimeElapsed = 0;
    }

    if (clusterIdle) {
      this.idleTimeElapsed += lastMeasuredTimestamp - oldTimeStamp;
    } else {
      this.idleTimeElapsed = 0;
    }

    if (overloadedTimeElapsed > scaleOutGracePeriod && taskStatsMap.getTaskList().size() < maxTaskNum) {
      return ScalingAction.SCALE_OUT;
    } else if (idleTimeElapsed > scaleInGracePeriod && taskStatsMap.getTaskList().size() > minTaskNum) {
      return ScalingAction.SCALE_IN;
    } else {
      return ScalingAction.NONE;
    }
  }
}
