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
package edu.snu.mist.core.configs;

import edu.snu.mist.core.master.lb.parameters.*;
import edu.snu.mist.core.master.lb.scaling.DoNothingDynamicScalingManager;
import edu.snu.mist.core.master.lb.scaling.DynamicScalingManager;
import edu.snu.mist.core.master.lb.scaling.PeriodicDynamicScalingManager;
import edu.snu.mist.core.master.recovery.parameters.RecoveryUnitSize;
import edu.snu.mist.core.parameters.*;
import edu.snu.mist.core.master.lb.allocation.*;
import edu.snu.mist.core.master.recovery.DistributedRecoveryScheduler;
import edu.snu.mist.core.master.recovery.RecoveryScheduler;
import edu.snu.mist.core.master.recovery.SingleNodeRecoveryScheduler;
import edu.snu.mist.core.rpc.DefaultClientToMasterMessageImpl;
import edu.snu.mist.core.rpc.DefaultDriverToMasterMessageImpl;
import edu.snu.mist.core.rpc.DefaultTaskToMasterMessageImpl;
import edu.snu.mist.formats.avro.ClientToMasterMessage;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * The tang configuration for master.
 */
public final class MistMasterConfigs implements MistConfigs {

  /**
   * The number of MistTasks.
   */
  private final int numTasks;

  /**
   * The memory size of a MistTask.
   */
  private final int taskMemSize;

  /**
   * The number of cores of a MistTask.
   */
  private final int numTaskCores;

  /**
   * The new ratio of the JVM process.
   */
  private final int newRatio;

  /**
   * The reserved code cache size of the JVM process.
   */
  private final int reservedCodeCacheSize;

  /**
   * The task load threshold for determining idle task.
   */
  private final double idleTaskThreshold;

  /**
   * The threshold for determining underloaded task.
   */
  private final double underloadedTaskThreshold;

  /**
   * The threshold for determining overloaded task.
   */
  private final double overloadedTaskThreshold;

  /**
   * The query allocation manager implementation option.
   */
  private final String queryAllocationOption;

  /**
   * The recovery scheduler implementation option.
   */
  private final String recoverySchedulerOption;

  /**
   * The granularity of recovery.
   */
  private final int recoveryUnitSize;

  // Start of variables for dynamic scaling in/out

  /**
   * The dynamic scaling policy.
   */
  private final String dynamicScalingOption;

  /**
   * The dynamic scailng period for periodic load balancing.
   * This variable has no meaning when other load balancing policy is used.
   */
  private final long dynamicScalingPeriod;

  /**
   * The grace period for scaling in for period load balancing.
   * This variable is disabled when other load balancing policy is used.
   */
  private final long scaleInGracePeriod;

  /**
   * The grace period for scaling in for period load balancing.
   * This variable is disabled when other load balancing policy is used.
   */
  private final long scaleOutGracePeriod;

  /**
   * The maximum task number for load balancing.
   */
  private final int maxTaskNum;

  /**
   * The minimum task number for load balancing.
   */
  private final int minTaskNum;

  /**
   * The rate of idle tasks for scaling-in.
   */
  private final double scaleInIdleTaskRate;

  /**
   * The rate of overloaded tasks for scaling-out.
   */
  private final double scaleOutOverloadedTaskRate;

  @Inject
  private MistMasterConfigs(
      @Parameter(NumTasks.class) final int numTasks,
      @Parameter(TaskMemorySize.class) final int taskMemSize,
      @Parameter(NumTaskCores.class) final int numTaskCores,
      @Parameter(NewRatio.class) final int newRatio,
      @Parameter(ReservedCodeCacheSize.class) final int reservedCodeCacheSize,
      @Parameter(IdleTaskLoadThreshold.class) final double idleTaskLoadThreshold,
      @Parameter(UnderloadedTaskLoadThreshold.class) final double underloadedTaskThreshold,
      @Parameter(OverloadedTaskLoadThreshold.class) final double overloadedTaskThreshold,
      @Parameter(QueryAllocationOption.class) final String queryAllocationOption,
      @Parameter(RecoverySchedulerOption.class) final String recoverySchedulerOption,
      @Parameter(RecoveryUnitSize.class) final int recoveryUnitSize,
      @Parameter(DynamicScalingOption.class) final String dynamicScalingOption,
      @Parameter(DynamicScalingPeriod.class) final long dynamicScalingPeriod,
      @Parameter(MaxTaskNum.class) final int maxTaskNum,
      @Parameter(MinTaskNum.class) final int minTaskNum,
      @Parameter(ScaleInGracePeriod.class) final long scaleInGracePeriod,
      @Parameter(ScaleInIdleTaskRate.class) final double scaleInIdleTaskRate,
      @Parameter(ScaleOutGracePeriod.class) final long scaleOutGracePeriod,
      @Parameter(ScaleOutOverloadedTaskRate.class) final double scaleOutOverloadedTaskRate) {
    this.numTasks = numTasks;
    this.taskMemSize = taskMemSize;
    this.numTaskCores = numTaskCores;
    this.newRatio = newRatio;
    this.reservedCodeCacheSize = reservedCodeCacheSize;
    this.idleTaskThreshold = idleTaskLoadThreshold;
    this.underloadedTaskThreshold = underloadedTaskThreshold;
    this.overloadedTaskThreshold = overloadedTaskThreshold;
    this.queryAllocationOption = queryAllocationOption;
    this.recoverySchedulerOption = recoverySchedulerOption;
    this.recoveryUnitSize = recoveryUnitSize;
    // Parameters for dynamic scale-in/out
    this.dynamicScalingOption = dynamicScalingOption;
    this.dynamicScalingPeriod = dynamicScalingPeriod;
    this.maxTaskNum = maxTaskNum;
    this.minTaskNum = minTaskNum;
    this.scaleInGracePeriod = scaleInGracePeriod;
    this.scaleInIdleTaskRate = scaleInIdleTaskRate;
    this.scaleOutGracePeriod = scaleOutGracePeriod;
    this.scaleOutOverloadedTaskRate = scaleOutOverloadedTaskRate;
  }

  private Class<? extends QueryAllocationManager> getQueryAllocationImplClass() {
    if (queryAllocationOption.equals("rr")) {
      return RoundRobinQueryAllocationManager.class;
    } else if (queryAllocationOption.equals("pot")) {
      return PowerOfTwoQueryAllocationManager.class;
    } else if (queryAllocationOption.equals("aa")) {
      return ApplicationAwareQueryAllocationManager.class;
    } else if (queryAllocationOption.equals("min")) {
      return MinLoadQueryAllocationManager.class;
    } else {
      throw new IllegalArgumentException("Invalid query allocation option!");
    }
  }

  private Class<? extends DynamicScalingManager> getDynamicScalingManagerImplClass() {
    if (dynamicScalingOption.equals("none")) {
      return DoNothingDynamicScalingManager.class;
    } else if (dynamicScalingOption.equals("periodic")) {
      return PeriodicDynamicScalingManager.class;
    } else {
      throw new IllegalArgumentException("Invalid dynamic scaling option!");
    }
  }

  @Override
  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    // Named parameters.
    jcb.bindNamedParameter(NumTasks.class, String.valueOf(numTasks));
    jcb.bindNamedParameter(TaskMemorySize.class, String.valueOf(taskMemSize));
    jcb.bindNamedParameter(NumTaskCores.class, String.valueOf(numTaskCores));
    jcb.bindNamedParameter(NewRatio.class, String.valueOf(newRatio));
    jcb.bindNamedParameter(ReservedCodeCacheSize.class, String.valueOf(reservedCodeCacheSize));
    jcb.bindNamedParameter(IdleTaskLoadThreshold.class, String.valueOf(idleTaskThreshold));
    jcb.bindNamedParameter(UnderloadedTaskLoadThreshold.class, String.valueOf(underloadedTaskThreshold));
    jcb.bindNamedParameter(OverloadedTaskLoadThreshold.class, String.valueOf(overloadedTaskThreshold));
    jcb.bindNamedParameter(RecoveryUnitSize.class, String.valueOf(recoveryUnitSize));
    jcb.bindNamedParameter(DynamicScalingPeriod.class, String.valueOf(dynamicScalingPeriod));
    jcb.bindNamedParameter(MaxTaskNum.class, String.valueOf(maxTaskNum));
    jcb.bindNamedParameter(MinTaskNum.class, String.valueOf(minTaskNum));
    jcb.bindNamedParameter(ScaleInGracePeriod.class, String.valueOf(scaleInGracePeriod));
    jcb.bindNamedParameter(ScaleInIdleTaskRate.class, String.valueOf(scaleInIdleTaskRate));
    jcb.bindNamedParameter(ScaleOutGracePeriod.class, String.valueOf(scaleOutGracePeriod));
    jcb.bindNamedParameter(ScaleOutOverloadedTaskRate.class, String.valueOf(scaleOutOverloadedTaskRate));

    // Implementations.
    jcb.bindImplementation(QueryAllocationManager.class, getQueryAllocationImplClass());
    jcb.bindImplementation(RecoveryScheduler.class, getRecoverySchedulerImplClass());
    jcb.bindImplementation(DynamicScalingManager.class, getDynamicScalingManagerImplClass());
    jcb.bindImplementation(ClientToMasterMessage.class, DefaultClientToMasterMessageImpl.class);
    jcb.bindImplementation(DriverToMasterMessage.class, DefaultDriverToMasterMessageImpl.class);
    jcb.bindImplementation(TaskToMasterMessage.class, DefaultTaskToMasterMessageImpl.class);
    return jcb.build();
  }

  private Class<? extends RecoveryScheduler> getRecoverySchedulerImplClass() {
    if (recoverySchedulerOption.equals("single")) {
      return SingleNodeRecoveryScheduler.class;
    } else if (recoverySchedulerOption.equals("distributed")) {
      return DistributedRecoveryScheduler.class;
    } else {
      throw new IllegalArgumentException("Invalid recovery scheduler option!");
    }
  }
}
