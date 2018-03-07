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

import edu.snu.mist.core.parameters.SharedStorePath;
import edu.snu.mist.core.parameters.TaskInfoGatherTerm;
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
   * Task information gathering term of MistMaster.
   */
  private final long taskInfoGatherTerm;

  @Inject
  private MistMasterConfigs(
      @Parameter(TaskInfoGatherTerm.class) final long taskInfoGatherTerm,
      @Parameter(SharedStorePath.class) final String sharedStorePath
  ) {
    this.taskInfoGatherTerm = taskInfoGatherTerm;
  }

  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TaskInfoGatherTerm.class, String.valueOf(taskInfoGatherTerm));
    return jcb.build();
  }

  public static CommandLine addCommandLineConf(final CommandLine commandLine) {
    final CommandLine cmd = commandLine
        .registerShortNameOfClass(TaskInfoGatherTerm.class);
    return cmd;
  }

}
