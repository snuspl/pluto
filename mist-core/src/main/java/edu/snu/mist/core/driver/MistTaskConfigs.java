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

import edu.snu.mist.common.rpc.AvroRPCNettyServerWrapper;
import edu.snu.mist.common.rpc.RPCServerPort;
import edu.snu.mist.common.shared.parameters.MqttSinkKeepAliveSec;
import edu.snu.mist.common.shared.parameters.MqttSourceKeepAliveSec;
import edu.snu.mist.core.parameters.TempFolderPath;
import edu.snu.mist.core.task.DefaultClientToTaskMessageImpl;
import edu.snu.mist.core.task.TaskSpecificResponderWrapper;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.GroupRebalancingPeriod;
import edu.snu.mist.core.task.groupaware.parameters.GroupPinningTime;
import edu.snu.mist.core.task.groupaware.parameters.ProcessingTimeout;
import edu.snu.mist.formats.avro.ClientToTaskMessage;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * Configuration of the mist task.
 */
public final class MistTaskConfigs {

  private static final int MAX_PORT_NUM = 65535;

  /**
   * The number of event processors of a MistTask.
   */
  private final int numEventProcessors;

  /**
   * Temporary folder path for storing temporay jar files.
   */
  private final String tempFolderPath;


  /**
   * The port number of rpc server of a MistTask.
   */
  private final int rpcServerPort;

  /**
   * The mqtt keep alive time for sources in seconds.
   */
  private final int mqttSourceKeepAliveSec;

  /**
   * The mqtt keep alive time for sinks in seconds.
   */
  private final int mqttSinkKeepAliveSec;

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

  @Inject
  private MistTaskConfigs(@Parameter(DefaultNumEventProcessors.class) final int numEventProcessors,
                          @Parameter(RPCServerPort.class) final int rpcServerPort,
                          @Parameter(TempFolderPath.class) final String tempFolderPath,
                          @Parameter(MqttSourceKeepAliveSec.class) final int mqttSourceKeepAliveSec,
                          @Parameter(MqttSinkKeepAliveSec.class) final int mqttSinkKeepAliveSec,
                          @Parameter(GroupRebalancingPeriod.class) final long rebalancingPeriod,
                          @Parameter(ProcessingTimeout.class) final long processingTimeout,
                          @Parameter(GroupPinningTime.class) final long groupPinningTime) {
    this.numEventProcessors = numEventProcessors;
    this.tempFolderPath = tempFolderPath;
    this.rpcServerPort = rpcServerPort + 10 > MAX_PORT_NUM ? rpcServerPort - 10 : rpcServerPort + 10;
    this.rebalancingPeriod = rebalancingPeriod;
    this.mqttSourceKeepAliveSec = mqttSourceKeepAliveSec;
    this.mqttSinkKeepAliveSec = mqttSinkKeepAliveSec;
    this.groupPinningTime = groupPinningTime;
    this.processingTimeout = processingTimeout;
  }

  /**
   * Get the task configuration.
   * @return configuration
   */
  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

    // Parameter
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, Integer.toString(numEventProcessors));
    jcb.bindNamedParameter(TempFolderPath.class, tempFolderPath);
    jcb.bindNamedParameter(RPCServerPort.class, Integer.toString(rpcServerPort));
    jcb.bindNamedParameter(MqttSourceKeepAliveSec.class, Integer.toString(mqttSourceKeepAliveSec));
    jcb.bindNamedParameter(MqttSinkKeepAliveSec.class, Integer.toString(mqttSinkKeepAliveSec));
    jcb.bindNamedParameter(GroupRebalancingPeriod.class, Long.toString(rebalancingPeriod));

    // Implementation
    jcb.bindImplementation(ClientToTaskMessage.class, DefaultClientToTaskMessageImpl.class);
    jcb.bindConstructor(Server.class, AvroRPCNettyServerWrapper.class);
    jcb.bindConstructor(SpecificResponder.class, TaskSpecificResponderWrapper.class);

    return jcb.build();
  }

  /**
   * Add parameters to the command line.
   * @param commandLine command line
   * @return command line where parameters are added
   */
  public static CommandLine addCommandLineConf(final CommandLine commandLine) {
    final CommandLine cmd = commandLine
        .registerShortNameOfClass(DefaultNumEventProcessors.class)
        .registerShortNameOfClass(TempFolderPath.class)
        .registerShortNameOfClass(MqttSourceKeepAliveSec.class)
        .registerShortNameOfClass(MqttSinkKeepAliveSec.class)
        .registerShortNameOfClass(ProcessingTimeout.class)
        .registerShortNameOfClass(GroupPinningTime.class)
        .registerShortNameOfClass(GroupRebalancingPeriod.class);

    return cmd;
  }
}
