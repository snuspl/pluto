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

import edu.snu.mist.common.rpc.RPCServerPort;
import edu.snu.mist.core.driver.parameters.NewRatio;
import edu.snu.mist.core.driver.parameters.ReservedCodeCacheSize;
import edu.snu.mist.core.parameters.NumTaskCores;
import edu.snu.mist.core.parameters.NumTasks;
import edu.snu.mist.core.parameters.TaskMemorySize;
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
   * The port number of rpc server of a MistTask.
   */
  private final int rpcServerPort;

  /**
   * The new ratio of the JVM process.
   */
  private final int newRatio;

  /**
   * The reserved code cache size of the JVM process.
   */
  private final int reservedCodeCacheSize;

  @Inject
  private MistDriverConfigs(@Parameter(NumTasks.class) final int numTasks,
                            @Parameter(TaskMemorySize.class) final int taskMemSize,
                            @Parameter(NumTaskCores.class) final int numTaskCores,
                            @Parameter(RPCServerPort.class) final int rpcServerPort,
                            @Parameter(NewRatio.class) final int newRatio,
                            @Parameter(ReservedCodeCacheSize.class) final int reservedCodeCacheSize) {
    this.numTasks = numTasks;
    this.taskMemSize = taskMemSize;
    this.numTaskCores = numTaskCores;
    this.rpcServerPort = rpcServerPort + 10 > MAX_PORT_NUM ? rpcServerPort - 10 : rpcServerPort + 10;
    this.newRatio = newRatio;
    this.reservedCodeCacheSize = reservedCodeCacheSize;
  }

  public int getNumTasks() {
   return numTasks;
  }

  public int getTaskMemSize() {
    return taskMemSize;
  }

  public int getNumTaskCores() {
    return numTaskCores;
  }

  public int getRpcServerPort() {
    return rpcServerPort;
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
    return commandLine.registerShortNameOfClass(NumTaskCores.class)
        .registerShortNameOfClass(NumTasks.class)
        .registerShortNameOfClass(RPCServerPort.class)
        .registerShortNameOfClass(TaskMemorySize.class)
        .registerShortNameOfClass(NewRatio.class)
        .registerShortNameOfClass(ReservedCodeCacheSize.class);
  }
}
