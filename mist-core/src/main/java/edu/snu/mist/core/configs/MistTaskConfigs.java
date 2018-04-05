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

import edu.snu.mist.core.rpc.DefaultClientToTaskMessageImpl;
import edu.snu.mist.core.rpc.DefaultMasterToTaskMessageImpl;
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
import edu.snu.mist.formats.avro.ClientToTaskMessage;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Configuration of the mist task.
 */
public final class MistTaskConfigs implements MistConfigs {

  /**
   * The number of event processors of a MistTask.
   */
  private final int numEventProcessors;

  /**
   * The mqtt keep alive time for sources in seconds.
   */
  private final int mqttSourceKeepAliveSec;

  /**
   * The mqtt keep alive time for sinks in seconds.
   */
  private final int mqttSinkKeepAliveSec;

  /**
   * The number of mqtt source clients per broker.
   */
  private final int mqttSourceClientNumPerBroker;

  /**
   * The number of mqtt sink clients per broker.
   */
  private final int mqttSinkClientNumPerBroker;

  /**
   * Group rebalancing period.
   */
  private final long rebalancingPeriod;

  /**
   * Group processing preemption timeout.
   */
  private final long processingTimeout;

  /**
   * Group pinning time.
   */
  private final long groupPinningTime;

  /**
   * The checkpoint period.
   */
  private final long checkpointPeriod;

  /**
   * The number of threads used for task-side recovery.
   */
  private final int recoveryThreadsNum;

  @Inject
  private MistTaskConfigs(@Parameter(DefaultNumEventProcessors.class) final int numEventProcessors,
                          @Parameter(MqttSourceKeepAliveSec.class) final int mqttSourceKeepAliveSec,
                          @Parameter(MqttSinkKeepAliveSec.class) final int mqttSinkKeepAliveSec,
                          @Parameter(MqttSourceClientNumPerBroker.class) final int mqttSourceClientNumPerBroker,
                          @Parameter(MqttSinkClientNumPerBroker.class) final int mqttSinkClientNumPerBroker,
                          @Parameter(GroupRebalancingPeriod.class) final long rebalancingPeriod,
                          @Parameter(ProcessingTimeout.class) final long processingTimeout,
                          @Parameter(GroupPinningTime.class) final long groupPinningTime,
                          @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod,
                          @Parameter(RecoveryThreadsNum.class) final int recoveryThreadsNum) {
    this.numEventProcessors = numEventProcessors;
    this.rebalancingPeriod = rebalancingPeriod;
    this.mqttSourceKeepAliveSec = mqttSourceKeepAliveSec;
    this.mqttSinkKeepAliveSec = mqttSinkKeepAliveSec;
    this.mqttSourceClientNumPerBroker = mqttSourceClientNumPerBroker;
    this.mqttSinkClientNumPerBroker = mqttSinkClientNumPerBroker;
    this.groupPinningTime = groupPinningTime;
    this.processingTimeout = processingTimeout;
    this.checkpointPeriod = checkpointPeriod;
    this.recoveryThreadsNum = recoveryThreadsNum;
  }

  /**
   * Get the task configuration.
   * @return configuration
   */
  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

    // Parameter
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, Integer.toString(numEventProcessors));
    jcb.bindNamedParameter(MqttSourceKeepAliveSec.class, Integer.toString(mqttSourceKeepAliveSec));
    jcb.bindNamedParameter(MqttSinkKeepAliveSec.class, Integer.toString(mqttSinkKeepAliveSec));
    jcb.bindNamedParameter(MqttSourceClientNumPerBroker.class, Integer.toString(mqttSourceClientNumPerBroker));
    jcb.bindNamedParameter(MqttSinkClientNumPerBroker.class, Integer.toString(mqttSinkClientNumPerBroker));
    jcb.bindNamedParameter(GroupRebalancingPeriod.class, Long.toString(rebalancingPeriod));
    jcb.bindNamedParameter(ProcessingTimeout.class, Long.toString(processingTimeout));
    jcb.bindNamedParameter(GroupPinningTime.class, Long.toString(groupPinningTime));
    jcb.bindNamedParameter(PeriodicCheckpointPeriod.class, Long.toString(checkpointPeriod));
    jcb.bindNamedParameter(RecoveryThreadsNum.class, Integer.toString(recoveryThreadsNum));

    // Implementation
    jcb.bindImplementation(ClientToTaskMessage.class, DefaultClientToTaskMessageImpl.class);
    jcb.bindImplementation(MasterToTaskMessage.class, DefaultMasterToTaskMessageImpl.class);
    return jcb.build();
  }
}
