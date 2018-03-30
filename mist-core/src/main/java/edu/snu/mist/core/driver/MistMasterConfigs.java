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
package edu.snu.mist.core.driver;

import edu.snu.mist.core.master.allocation.*;
import edu.snu.mist.core.master.recovery.DistributedRecoveryScheduler;
import edu.snu.mist.core.master.recovery.RecoveryScheduler;
import edu.snu.mist.core.master.recovery.SingleNodeRecoveryScheduler;
import edu.snu.mist.core.parameters.OverloadedTaskThreshold;
import edu.snu.mist.core.parameters.QueryAllocationOption;
import edu.snu.mist.core.parameters.RecoverySchedulerOption;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * The tang configuration for master.
 */
public final class MistMasterConfigs {

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

  @Inject
  private MistMasterConfigs(
      @Parameter(OverloadedTaskThreshold.class) final double overloadedTaskThreshold,
      @Parameter(QueryAllocationOption.class) final String queryAllocationOption,
      @Parameter(RecoverySchedulerOption.class) final String recoverySchedulerOption) {
    this.overloadedTaskThreshold = overloadedTaskThreshold;
    this.queryAllocationOption = queryAllocationOption;
    this.recoverySchedulerOption = recoverySchedulerOption;
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

  private Class<? extends RecoveryScheduler> getRecoverySchedulerImplClass() {
    if (recoverySchedulerOption.equals("single")) {
      return SingleNodeRecoveryScheduler.class;
    } else if (recoverySchedulerOption.equals("distributed")) {
      return DistributedRecoveryScheduler.class;
    } else {
      throw new IllegalArgumentException("Invalid recovery scheduler option!");
    }
  }

  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(OverloadedTaskThreshold.class, String.valueOf(overloadedTaskThreshold));
    jcb.bindImplementation(QueryAllocationManager.class, getQueryAllocationImplClass());
    jcb.bindImplementation(RecoveryScheduler.class, getRecoverySchedulerImplClass());
    return jcb.build();
  }

  public static CommandLine addCommandLineConf(final CommandLine commandLine) {
    return commandLine
        .registerShortNameOfClass(OverloadedTaskThreshold.class)
        .registerShortNameOfClass(QueryAllocationOption.class)
        .registerShortNameOfClass(RecoverySchedulerOption.class);
  }
}
