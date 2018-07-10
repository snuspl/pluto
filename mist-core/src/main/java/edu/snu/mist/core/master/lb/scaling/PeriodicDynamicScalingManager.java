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

import edu.snu.mist.core.master.TaskInfoRWLock;
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.lb.parameters.DynamicScalingPeriod;
import edu.snu.mist.core.master.lb.parameters.IdleTaskLoadThreshold;
import edu.snu.mist.core.master.lb.parameters.MaxTaskNum;
import edu.snu.mist.core.master.lb.parameters.MinTaskNum;
import edu.snu.mist.core.master.lb.parameters.OverloadedTaskLoadThreshold;
import edu.snu.mist.core.master.lb.parameters.ScaleInGracePeriod;
import edu.snu.mist.core.master.lb.parameters.ScaleInIdleTaskRatio;
import edu.snu.mist.core.master.lb.parameters.ScaleOutGracePeriod;
import edu.snu.mist.core.master.lb.parameters.ScaleOutOverloadedTaskRatio;
import edu.snu.mist.core.master.recovery.RecoveryLock;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The default implementation for dynamic scaling policy.
 */
public final class PeriodicDynamicScalingManager implements DynamicScalingManager {

  private static final Logger LOG = Logger.getLogger(PeriodicDynamicScalingManager.class.getName());

  /**
   * The shared stats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The period of performing dynamic scaling.
   */
  private final long dynamicScalingPeriod;

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
   * The ratio of idle tasks for determining scaling-in action.
   */
  private final double scaleInIdleTaskRatio;

  /**
   * The ratio of overloaded tasks for determining scaling-out action.
   */
  private final double scaleOutOverloadedTaskRatio;

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

  /**
   * Scheduled executor service for running periodic dynamic scaling.
   */
  private ScheduledExecutorService scheduledExecutorService;

  /**
   * The scale-in manager.
   */
  private final ScaleInManager scaleInManager;

  /**
   * The shared lock for synchronizing recovery process.
   */
  private final RecoveryLock recoveryLock;

  /**
   * The read/write lock for task info update.
   */
  private final TaskInfoRWLock taskInfoRWLock;

  @Inject
  private PeriodicDynamicScalingManager(
      final TaskStatsMap taskStatsMap,
      @Parameter(DynamicScalingPeriod.class) final long dynamicScalingPeriod,
      @Parameter(MaxTaskNum.class) final int maxTaskNum,
      @Parameter(MinTaskNum.class) final int minTaskNum,
      @Parameter(IdleTaskLoadThreshold.class) final double idleTaskLoadThreshold,
      @Parameter(OverloadedTaskLoadThreshold.class) final double overloadedTaskLoadThreshold,
      @Parameter(ScaleInGracePeriod.class) final long scaleInGracePeriod,
      @Parameter(ScaleOutGracePeriod.class) final long scaleOutGracePeriod,
      @Parameter(ScaleInIdleTaskRatio.class) final double scaleInIdleTaskRatio,
      @Parameter(ScaleOutOverloadedTaskRatio.class) final double scaleOutOverloadedTaskRatio,
      final ScaleInManager scaleInManager,
      final RecoveryLock recoveryLock,
      final TaskInfoRWLock taskInfoRWLock) {
    this.taskStatsMap = taskStatsMap;
    this.dynamicScalingPeriod = dynamicScalingPeriod;
    this.maxTaskNum = maxTaskNum;
    this.minTaskNum = minTaskNum;
    this.idleTaskLoadThreshold = idleTaskLoadThreshold;
    this.overloadedTaskLoadThreshold = overloadedTaskLoadThreshold;
    this.scaleInGracePeriod = scaleInGracePeriod;
    this.scaleOutGracePeriod = scaleOutGracePeriod;
    this.scaleInIdleTaskRatio = scaleInIdleTaskRatio;
    this.scaleOutOverloadedTaskRatio = scaleOutOverloadedTaskRatio;
    this.idleTimeElapsed = 0L;
    this.overloadedTimeElapsed = 0L;
    this.lastMeasuredTimestamp = System.currentTimeMillis();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.scaleInManager = scaleInManager;
    this.recoveryLock = recoveryLock;
    this.taskInfoRWLock = taskInfoRWLock;
  }

  private boolean isClusterOverloaded() {
    int overloadedTaskNum = 0;
    for (final Map.Entry<String, TaskStats> entry : taskStatsMap.entrySet()) {
      if (entry.getValue().getTaskLoad() > overloadedTaskLoadThreshold) {
        overloadedTaskNum += 1;
      }
    }
    final double overloadedTaskRatio = overloadedTaskNum / taskStatsMap.getTaskList().size();
    return overloadedTaskRatio > scaleOutOverloadedTaskRatio;
  }

  private boolean isClusterIdle() {
    int idleTaskNum = 0;
    for (final Map.Entry<String, TaskStats> entry : taskStatsMap.entrySet()) {
      if (entry.getValue().getTaskLoad() <= idleTaskLoadThreshold) {
        idleTaskNum += 1;
      }
    }
    final double idleTaskRatio = idleTaskNum / taskStatsMap.getTaskList().size();
    return idleTaskRatio > scaleInIdleTaskRatio;
  }

  private final class AutoScaleRunner implements Runnable {

    private AutoScaleRunner() {
      // Do nothing.
    }

    @Override
    public void run() {
      taskInfoRWLock.readLock().lock();
      final long oldTimeStamp = lastMeasuredTimestamp;
      lastMeasuredTimestamp = System.currentTimeMillis();

      final boolean clusterOverloaded = isClusterOverloaded();
      final boolean clusterIdle = isClusterIdle();

      LOG.log(Level.INFO, "Task Num = {0}. Overloaded = {1}, Idle = {2}",
          new Object[]{taskStatsMap.entrySet().size(), clusterOverloaded, clusterIdle});

      if (clusterOverloaded) {
        overloadedTimeElapsed += lastMeasuredTimestamp - oldTimeStamp;
        if (overloadedTimeElapsed > scaleOutGracePeriod && taskStatsMap.getTaskList().size() < maxTaskNum
            && recoveryLock.tryLock()) {
          try {
            // Release the lock.
            taskInfoRWLock.readLock().unlock();
            // TODO: [MIST-1130] Perform automatic scale-out.
            overloadedTimeElapsed = 0;
          } catch (final Exception e) {
            e.printStackTrace();
          } finally {
            recoveryLock.unlock();
          }
          return;
        }
      } else {
        overloadedTimeElapsed = 0;
      }

      if (clusterIdle) {
        idleTimeElapsed += lastMeasuredTimestamp - oldTimeStamp;
        // Try to acquire recovery lock to prevent fault recovery during automatic scaling.
        if (idleTimeElapsed > scaleInGracePeriod && taskStatsMap.getTaskList().size() > minTaskNum
            && recoveryLock.tryLock()) {
          LOG.log(Level.INFO, "Start scaling-in...");
          try {
            // Release the lock.
            taskInfoRWLock.readLock().unlock();
            scaleInManager.scaleIn();
            // Initialize the idleTimeElapsed.
            idleTimeElapsed = 0;
          } catch (final Exception e) {
            e.printStackTrace();
          } finally {
            recoveryLock.unlock();
          }
        }
        return;
      } else {
        idleTimeElapsed = 0;
      }
      taskInfoRWLock.readLock().unlock();
    }
  }

  @Override
  public void startAutoScaling() {
    scheduledExecutorService.scheduleAtFixedRate(new AutoScaleRunner(),
        dynamicScalingPeriod, dynamicScalingPeriod, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() throws Exception {
    scaleInManager.close();
    scheduledExecutorService.shutdown();
    scheduledExecutorService.awaitTermination(6000, TimeUnit.MILLISECONDS);
  }
}
