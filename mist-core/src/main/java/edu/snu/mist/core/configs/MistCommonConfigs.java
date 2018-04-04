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

import edu.snu.mist.core.parameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * The common configurations shared among MistMaster and MistTasks.
 */
public final class MistCommonConfigs implements MistConfigs {

  /**
   * The client-to-master RPC port number.
   */
  private final int clientToMasterPort;

  /**
   * The client-to-task RPC port number.
   */
  private final int clientToTaskPort;

  /**
   * The master-to-task RPC port number.
   */
  private final int masterToTaskPort;

  /**
   * The master-to-driver RPC port number.
   */
  private final int masterToDriverPort;

  /**
   * The task-to-master RPC port number.
   */
  private final int taskToMasterPort;

  /**
   * The driver-to-master RPC port number.
   */
  private final int driverToMasterPort;

  /**
   * Temporary folder path for storing temporay jar files.
   */
  private final String sharedStorePath;

  @Inject
  private MistCommonConfigs(
      @Parameter(ClientToMasterPort.class) final int clientToMasterPort,
      @Parameter(ClientToTaskPort.class) final int clientToTaskPort,
      @Parameter(MasterToTaskPort.class) final int masterToTaskPort,
      @Parameter(MasterToDriverPort.class) final int masterToDriverPort,
      @Parameter(TaskToMasterPort.class) final int taskToMasterPort,
      @Parameter(DriverToMasterPort.class) final int driverToMasterPort,
      @Parameter(SharedStorePath.class) final String sharedStorePath) {
    this.clientToMasterPort = clientToMasterPort;
    this.clientToTaskPort = clientToTaskPort;
    this.masterToTaskPort = masterToTaskPort;
    this.masterToDriverPort = masterToDriverPort;
    this.taskToMasterPort = taskToMasterPort;
    this.driverToMasterPort = driverToMasterPort;
    this.sharedStorePath = sharedStorePath;
  }

  @Override
  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(ClientToMasterPort.class, String.valueOf(clientToMasterPort));
    jcb.bindNamedParameter(ClientToTaskPort.class, String.valueOf(clientToTaskPort));
    jcb.bindNamedParameter(MasterToTaskPort.class, String.valueOf(masterToTaskPort));
    jcb.bindNamedParameter(MasterToDriverPort.class, String.valueOf(masterToDriverPort));
    jcb.bindNamedParameter(TaskToMasterPort.class, String.valueOf(taskToMasterPort));
    jcb.bindNamedParameter(DriverToMasterPort.class, String.valueOf(driverToMasterPort));
    jcb.bindNamedParameter(SharedStorePath.class, sharedStorePath);
    return jcb.build();
  }
}
