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
import edu.snu.mist.core.master.recovery.parameters.RecoveryUnitSize;
import edu.snu.mist.core.parameters.*;
import edu.snu.mist.core.shared.parameters.MqttSinkClientNumPerBroker;
import edu.snu.mist.core.shared.parameters.MqttSinkKeepAliveSec;
import edu.snu.mist.core.shared.parameters.MqttSourceClientNumPerBroker;
import edu.snu.mist.core.shared.parameters.MqttSourceKeepAliveSec;
import edu.snu.mist.core.sources.parameters.PeriodicCheckpointPeriod;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.GroupRebalancingPeriod;
import edu.snu.mist.core.task.groupaware.parameters.GroupPinningTime;
import edu.snu.mist.core.task.groupaware.parameters.ProcessingTimeout;
import edu.snu.mist.core.task.recovery.parameters.RecoveryThreadsNum;
import org.apache.reef.tang.formats.CommandLine;

/**
 * The class which registers all the command line options in MIST.
 */
public final class MistCommandLineOptions {

  private MistCommandLineOptions() {
    // Should not be called.
  }

  /**
   * Add the command line configurations for MIST.
   * @param commandLine
   * @return
   */
  public static CommandLine addCommandLineConf(final CommandLine commandLine) {
    return commandLine
        .registerShortNameOfClass(ClientToMasterPort.class)
        .registerShortNameOfClass(ClientToTaskPort.class)
        .registerShortNameOfClass(MasterToTaskPort.class)
        .registerShortNameOfClass(TaskToMasterPort.class)
        .registerShortNameOfClass(DriverToMasterPort.class)
        .registerShortNameOfClass(SharedStorePath.class)
        .registerShortNameOfClass(NumTaskCores.class)
        .registerShortNameOfClass(NumTasks.class)
        .registerShortNameOfClass(TaskMemorySize.class)
        .registerShortNameOfClass(NumMasterCores.class)
        .registerShortNameOfClass(MasterMemorySize.class)
        .registerShortNameOfClass(NewRatio.class)
        .registerShortNameOfClass(ReservedCodeCacheSize.class)
        .registerShortNameOfClass(DefaultNumEventProcessors.class)
        .registerShortNameOfClass(MqttSourceKeepAliveSec.class)
        .registerShortNameOfClass(MqttSinkKeepAliveSec.class)
        .registerShortNameOfClass(MqttSourceClientNumPerBroker.class)
        .registerShortNameOfClass(MqttSinkClientNumPerBroker.class)
        .registerShortNameOfClass(ProcessingTimeout.class)
        .registerShortNameOfClass(GroupPinningTime.class)
        .registerShortNameOfClass(GroupRebalancingPeriod.class)
        .registerShortNameOfClass(PeriodicCheckpointPeriod.class)
        .registerShortNameOfClass(UnderloadedTaskLoadThreshold.class)
        .registerShortNameOfClass(OverloadedTaskLoadThreshold.class)
        .registerShortNameOfClass(QueryAllocationOption.class)
        .registerShortNameOfClass(RecoveryThreadsNum.class)
        .registerShortNameOfClass(RecoverySchedulerOption.class)
        .registerShortNameOfClass(RecoveryUnitSize.class)
        .registerShortNameOfClass(DynamicScalingOption.class)
        .registerShortNameOfClass(DynamicScalingPeriod.class)
        .registerShortNameOfClass(IdleTaskLoadThreshold.class)
        .registerShortNameOfClass(MaxTaskNum.class)
        .registerShortNameOfClass(MinTaskNum.class)
        .registerShortNameOfClass(OverloadedTaskLoadThreshold.class)
        .registerShortNameOfClass(ScaleInGracePeriod.class)
        .registerShortNameOfClass(ScaleInIdleTaskRatio.class)
        .registerShortNameOfClass(ScaleOutGracePeriod.class)
        .registerShortNameOfClass(ScaleOutOverloadedTaskRatio.class);
  }
}
