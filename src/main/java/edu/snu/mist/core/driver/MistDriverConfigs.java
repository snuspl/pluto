/*
 * Copyright (C) 2017 Seoul National University
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

import edu.snu.mist.core.driver.parameters.AvroRPCPortStart;
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
   * The number of Mist Masters.
   */
  private final int numMasters;

  /**
   * The number of MistTasks.
   */
  private final int numTasks;

  /**
   * The memory size of a Mist master.
   */
  private final int masterMemSize;

  /**
   * The memory size of a MistTask.
   */
  private final int taskMemSize;

  /**
   * The number of cores of a Mist master.
   */
  private final int numMasterCores;

  /**
   * The number of cores of a MistTask.
   */
  private final int numTaskCores;

  /**
   * The start port number of rpc servers.
   */
  private final int avroRpcServerPortStart;

  /**
   * The new ratio of the JVM process.
   */
  private final int newRatio;

  /**
   * The reserved code cache size of the JVM process.
   */
  private final int reservedCodeCacheSize;

  @Inject
  private MistDriverConfigs(@Parameter(NumMasters.class) final int numMasters,
                            @Parameter(NumTasks.class) final int numTasks,
                            @Parameter(MasterMemorySize.class) final int masterMemSize,
                            @Parameter(TaskMemorySize.class) final int taskMemSize,
                            @Parameter(NumMasterCores.class) final int numMasterCores,
                            @Parameter(NumTaskCores.class) final int numTaskCores,
                            @Parameter(AvroRPCPortStart.class) final int avroRpcServerPortStart,
                            @Parameter(NewRatio.class) final int newRatio,
                            @Parameter(ReservedCodeCacheSize.class) final int reservedCodeCacheSize) {
    this.numMasters = numMasters;
    this.numTasks = numTasks;
    this.masterMemSize = masterMemSize;
    this.taskMemSize = taskMemSize;
    this.numMasterCores = numMasterCores;
    this.numTaskCores = numTaskCores;
    this.avroRpcServerPortStart = avroRpcServerPortStart;
    this.newRatio = newRatio;
    this.reservedCodeCacheSize = reservedCodeCacheSize;
  }

  public int getNumMasters() {
    return numMasters;
  }

  public int getNumTasks() {
   return numTasks;
  }

  public int getMasterMemSize() {
    return masterMemSize;
  }

  public int getTaskMemSize() {
    return taskMemSize;
  }

  public int getNumMasterCores() {
    return numMasterCores;
  }

  public int getNumTaskCores() {
    return numTaskCores;
  }

  public int getAvroRpcServerPortStart() {
    return avroRpcServerPortStart;
  }

  public int getNewRatio() {
    return this.newRatio;
  }

  public int getReservedCodeCacheSize() {
    return this.reservedCodeCacheSize;
  }

  /**
   * Add parameters to the command line.
   * @param commandLine command line
   * @return command line where parameters are added
   */
  public static CommandLine addCommandLineConf(final CommandLine commandLine) {
    return commandLine
        .registerShortNameOfClass(NumMasterCores.class)
        .registerShortNameOfClass(NumTaskCores.class)
        .registerShortNameOfClass(NumMasters.class)
        .registerShortNameOfClass(NumTasks.class)
        .registerShortNameOfClass(MasterMemorySize.class)
        .registerShortNameOfClass(TaskMemorySize.class)
        .registerShortNameOfClass(NewRatio.class)
        .registerShortNameOfClass(ReservedCodeCacheSize.class);
  }
}
