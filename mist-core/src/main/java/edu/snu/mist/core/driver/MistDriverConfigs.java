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

import edu.snu.mist.core.driver.parameters.NewRatio;
import edu.snu.mist.core.driver.parameters.ReservedCodeCacheSize;
import edu.snu.mist.core.parameters.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * This class contains information necessary for driver.
 */
public final class MistDriverConfigs {

  private static final int MAX_PORT_NUM = 65535;

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
   * The memory size of a MistMaster.
   */
  private final int masterMemSize;

  /**
   * The number of cores of a MistMaster.
   */
  private final int numMasterCores;

  /**
   * The new ratio of the JVM process.
   */
  private final int newRatio;

  /**
   * The reserved code cache size of the JVM process.
   */
  private final int reservedCodeCacheSize;

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
  private MistDriverConfigs(@Parameter(NumTasks.class) final int numTasks,
                            @Parameter(TaskMemorySize.class) final int taskMemSize,
                            @Parameter(NumTaskCores.class) final int numTaskCores,
                            @Parameter(MasterMemorySize.class) final int masterMemSize,
                            @Parameter(NumMasterCores.class) final int numMasterCores,
                            @Parameter(NewRatio.class) final int newRatio,
                            @Parameter(ReservedCodeCacheSize.class) final int reservedCodeCacheSize,
                            @Parameter(ClientToMasterPort.class) final int clientToMasterPort,
                            @Parameter(ClientToTaskPort.class) final int clientToTaskPort,
                            @Parameter(MasterToTaskPort.class) final int masterToTaskPort,
                            @Parameter(TaskToMasterPort.class) final int taskToMasterPort,
                            @Parameter(DriverToMasterPort.class) final int driverToMasterPort,
                            @Parameter(SharedStorePath.class) final String sharedStorePath) {
    this.numTasks = numTasks;
    this.taskMemSize = taskMemSize;
    this.numTaskCores = numTaskCores;
    this.masterMemSize = masterMemSize;
    this.numMasterCores = numMasterCores;
    this.newRatio = newRatio;
    this.reservedCodeCacheSize = reservedCodeCacheSize;
    this.clientToMasterPort = clientToMasterPort;
    this.clientToTaskPort = clientToTaskPort;
    this.masterToTaskPort = masterToTaskPort;
    this.taskToMasterPort = taskToMasterPort;
    this.driverToMasterPort = driverToMasterPort;
    this.sharedStorePath = sharedStorePath;
  }

  public int getNumTasks() {
   return this.numTasks;
  }

  public int getTaskMemSize() {
    return this.taskMemSize;
  }

  public int getNumTaskCores() {
    return this.numTaskCores;
  }

  public int getMasterMemSize() {
    return this.masterMemSize;
  }

  public int getNumMasterCores() {
    return this.numMasterCores;
  }

  public int getNewRatio() {
    return this.newRatio;
  }

  public int getReservedCodeCacheSize() {
    return this.reservedCodeCacheSize;
  }

  public int getClientToMasterPort() {
    return this.clientToMasterPort;
  }

  public int getClientToTaskPort() {
    return this.clientToTaskPort;
  }

  public int getMasterToTaskPort() {
    return this.masterToTaskPort;
  }

  public int getTaskToMasterPort() {
    return this.taskToMasterPort;
  }

  public int getDriverToMasterPort() {
    return this.driverToMasterPort;
  }

  public String getSharedStorePath() {
    return this.sharedStorePath;
  }

  /**
   * Add parameters to the command line.
   * @param commandLine command line
   * @return command line where parameters are added
   */
  public static CommandLine addCommandLineConf(final CommandLine commandLine) {
    return commandLine.registerShortNameOfClass(NumTaskCores.class)
        .registerShortNameOfClass(NumTasks.class)
        .registerShortNameOfClass(TaskMemorySize.class)
        .registerShortNameOfClass(NumMasterCores.class)
        .registerShortNameOfClass(MasterMemorySize.class)
        .registerShortNameOfClass(NewRatio.class)
        .registerShortNameOfClass(ReservedCodeCacheSize.class)
        .registerShortNameOfClass(ClientToMasterPort.class)
        .registerShortNameOfClass(ClientToTaskPort.class)
        .registerShortNameOfClass(MasterToTaskPort.class)
        .registerShortNameOfClass(TaskToMasterPort.class)
        .registerShortNameOfClass(DriverToMasterPort.class)
        .registerShortNameOfClass(SharedStorePath.class);
  }
}
