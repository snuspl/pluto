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

import edu.snu.mist.core.parameters.NewRatio;
import edu.snu.mist.core.parameters.ReservedCodeCacheSize;
import edu.snu.mist.core.master.allocation.*;
import edu.snu.mist.core.master.allocation.parameters.OverloadedTaskThreshold;
import edu.snu.mist.core.parameters.NumTaskCores;
import edu.snu.mist.core.parameters.NumTasks;
import edu.snu.mist.core.parameters.QueryAllocationOption;
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

  @Inject
  private MistMasterConfigs(
      @Parameter(NumTasks.class) final int numTasks,
      @Parameter(TaskMemorySize.class) final int taskMemSize,
      @Parameter(NumTaskCores.class) final int numTaskCores,
      @Parameter(NewRatio.class) final int newRatio,
      @Parameter(ReservedCodeCacheSize.class) final int reservedCodeCacheSize,
      @Parameter(OverloadedTaskThreshold.class) final double overloadedTaskThreshold,
      @Parameter(QueryAllocationOption.class) final String queryAllocationOption) {
    this.numTasks = numTasks;
    this.taskMemSize = taskMemSize;
    this.numTaskCores = numTaskCores;
    this.newRatio = newRatio;
    this.reservedCodeCacheSize = reservedCodeCacheSize;
    this.overloadedTaskThreshold = overloadedTaskThreshold;
    this.queryAllocationOption = queryAllocationOption;
  }

  @Override
  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(NumTasks.class, String.valueOf(numTasks));
    jcb.bindNamedParameter(TaskMemorySize.class, String.valueOf(taskMemSize));
    jcb.bindNamedParameter(NumTaskCores.class, String.valueOf(numTaskCores));
    jcb.bindNamedParameter(NewRatio.class, String.valueOf(newRatio));
    jcb.bindNamedParameter(ReservedCodeCacheSize.class, String.valueOf(reservedCodeCacheSize));
    jcb.bindNamedParameter(OverloadedTaskThreshold.class, String.valueOf(overloadedTaskThreshold));
    if (queryAllocationOption.equals("rr")) {
      jcb.bindImplementation(QueryAllocationManager.class, RoundRobinQueryAllocationManager.class);
    } else if (queryAllocationOption.equals("pot")) {
      jcb.bindImplementation(QueryAllocationManager.class, PowerOfTwoQueryAllocationManager.class);
    } else if (queryAllocationOption.equals("aa")) {
      jcb.bindImplementation(QueryAllocationManager.class, ApplicationAwareQueryAllocationManager.class);
    } else if (queryAllocationOption.equals("min")) {
      jcb.bindImplementation(QueryAllocationManager.class, MinLoadQueryAllocationManager.class);
    } else {
      throw new IllegalArgumentException("Invalid query allocation option!");
    }
    jcb.bindImplementation(DriverToMasterMessage.class, DefaultDriverToMasterMessageImpl.class);
    jcb.bindImplementation(ClientToMasterMessage.class, DefaultClientToMasterMessageImpl.class);
    jcb.bindImplementation(TaskToMasterMessage.class, DefaultTaskToMasterMessageImpl.class);
    return jcb.build();
  }
}
