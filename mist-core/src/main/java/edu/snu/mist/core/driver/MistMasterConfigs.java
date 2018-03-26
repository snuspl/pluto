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
import edu.snu.mist.core.parameters.OverloadedTaskThreshold;
import edu.snu.mist.core.parameters.QueryAllocationOption;
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
   * The query allocation manager implementation option.
   */
  private final String queryAllocationOption;

  @Inject
  private MistMasterConfigs(
      @Parameter(TaskInfoGatherPeriod.class) final long taskInfoGatherPeriod,
      @Parameter(OverloadedTaskThreshold.class) final double overloadedTaskThreshold,
      @Parameter(QueryAllocationOption.class) final String queryAllocationOption) {
    this.taskInfoGatherPeriod = taskInfoGatherPeriod;
    this.overloadedTaskThreshold = overloadedTaskThreshold;
    this.queryAllocationOption = queryAllocationOption;
  }

  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TaskInfoGatherPeriod.class, String.valueOf(taskInfoGatherPeriod));
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
    return jcb.build();
  }

  public static CommandLine addCommandLineConf(final CommandLine commandLine) {
    final CommandLine cmd = commandLine
        .registerShortNameOfClass(TaskInfoGatherPeriod.class)
        .registerShortNameOfClass(OverloadedTaskThreshold.class)
        .registerShortNameOfClass(QueryAllocationOption.class);
    return cmd;
  }
}
