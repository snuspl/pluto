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

import edu.snu.mist.core.master.recovery.parameters.RecoveryUnitSize;
import edu.snu.mist.core.parameters.NewRatio;
import edu.snu.mist.core.parameters.ReservedCodeCacheSize;
import edu.snu.mist.core.master.allocation.*;
import edu.snu.mist.core.master.recovery.DistributedRecoveryScheduler;
import edu.snu.mist.core.master.recovery.RecoveryScheduler;
import edu.snu.mist.core.master.recovery.SingleNodeRecoveryScheduler;
import edu.snu.mist.core.master.allocation.parameters.OverloadedTaskThreshold;
import edu.snu.mist.core.parameters.QueryAllocationOption;
import edu.snu.mist.core.parameters.RecoverySchedulerOption;
import edu.snu.mist.core.parameters.NumTaskCores;
import edu.snu.mist.core.parameters.NumTasks;
import edu.snu.mist.core.parameters.TaskMemorySize;
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

  @Inject
  private MistMasterConfigs(
      @Parameter(NumTasks.class) final int numTasks,
      @Parameter(TaskMemorySize.class) final int taskMemSize,
      @Parameter(NumTaskCores.class) final int numTaskCores,
      @Parameter(NewRatio.class) final int newRatio,
      @Parameter(ReservedCodeCacheSize.class) final int reservedCodeCacheSize,
      @Parameter(OverloadedTaskThreshold.class) final double overloadedTaskThreshold,
      @Parameter(QueryAllocationOption.class) final String queryAllocationOption,
      @Parameter(RecoverySchedulerOption.class) final String recoverySchedulerOption,
      @Parameter(RecoveryUnitSize.class) final int recoveryUnitSize) {
    this.numTasks = numTasks;
    this.taskMemSize = taskMemSize;
    this.numTaskCores = numTaskCores;
    this.newRatio = newRatio;
    this.reservedCodeCacheSize = reservedCodeCacheSize;
    this.overloadedTaskThreshold = overloadedTaskThreshold;
    this.queryAllocationOption = queryAllocationOption;
    this.recoverySchedulerOption = recoverySchedulerOption;
    this.recoveryUnitSize = recoveryUnitSize;
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

  @Override
  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    // Named parameters.
    jcb.bindNamedParameter(NumTasks.class, String.valueOf(numTasks));
    jcb.bindNamedParameter(TaskMemorySize.class, String.valueOf(taskMemSize));
    jcb.bindNamedParameter(NumTaskCores.class, String.valueOf(numTaskCores));
    jcb.bindNamedParameter(NewRatio.class, String.valueOf(newRatio));
    jcb.bindNamedParameter(ReservedCodeCacheSize.class, String.valueOf(reservedCodeCacheSize));
    jcb.bindNamedParameter(OverloadedTaskThreshold.class, String.valueOf(overloadedTaskThreshold));
    jcb.bindNamedParameter(RecoveryUnitSize.class, String.valueOf(recoveryUnitSize));

    // Implementations.
    jcb.bindImplementation(QueryAllocationManager.class, getQueryAllocationImplClass());
    jcb.bindImplementation(RecoveryScheduler.class, getRecoverySchedulerImplClass());
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
