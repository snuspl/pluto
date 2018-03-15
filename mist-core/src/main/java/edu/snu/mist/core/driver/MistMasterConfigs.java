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

import edu.snu.mist.core.master.DistributedMasterRecoveryManager;
import edu.snu.mist.core.master.MasterRecoveryManager;
import edu.snu.mist.core.master.SingleNodeMasterRecoveryManager;
import edu.snu.mist.core.parameters.DistributedRecoveryOn;
import edu.snu.mist.core.parameters.OverloadedTaskThreshold;
import edu.snu.mist.core.parameters.TaskInfoGatherPeriod;
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
   * Task information gathering period of MistMaster.
   */
  private final long taskInfoGatherPeriod;

  /**
   * The threshold for determining overloaded task.
   */
  private final double overloadedTaskThreshold;

  /**
   * Indicates whether the distributed recovery is on or not.
   */
  private final boolean distributedRecoveryOn;

  @Inject
  private MistMasterConfigs(
      @Parameter(TaskInfoGatherPeriod.class) final long taskInfoGatherPeriod,
      @Parameter(OverloadedTaskThreshold.class) final double overloadedTaskThreshold,
      @Parameter(DistributedRecoveryOn.class) final boolean distributedRecoveryOn) {
    this.taskInfoGatherPeriod = taskInfoGatherPeriod;
    this.overloadedTaskThreshold = overloadedTaskThreshold;
    this.distributedRecoveryOn = distributedRecoveryOn;
  }

  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TaskInfoGatherPeriod.class, String.valueOf(taskInfoGatherPeriod));
    jcb.bindNamedParameter(OverloadedTaskThreshold.class, String.valueOf(overloadedTaskThreshold));
    if (distributedRecoveryOn) {
      jcb.bindImplementation(MasterRecoveryManager.class, DistributedMasterRecoveryManager.class);
    } else {
      jcb.bindImplementation(MasterRecoveryManager.class, SingleNodeMasterRecoveryManager.class);
    }
    return jcb.build();
  }

  public static CommandLine addCommandLineConf(final CommandLine commandLine) {
    final CommandLine cmd = commandLine
        .registerShortNameOfClass(TaskInfoGatherPeriod.class)
        .registerShortNameOfClass(OverloadedTaskThreshold.class)
        .registerShortNameOfClass(DistributedRecoveryOn.class);
    return cmd;
  }
}
