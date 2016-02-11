/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.driver;

import edu.snu.mist.common.rpc.RPCServerPort;
import edu.snu.mist.driver.parameters.NumTaskCores;
import edu.snu.mist.driver.parameters.NumTasks;
import edu.snu.mist.driver.parameters.TaskMemorySize;
import edu.snu.mist.task.parameters.NumExecutors;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * This class contains information for setting MistTasks.
 */
final class MistTaskConfigs {

  /**
   * The number of MistTasks.
   */
  private final int numTasks;

  /**
   * The memory size of a MistTask.
   */
  private final int taskMemSize;

  /**
   * The number of executors of a MistTask.
   */
  private final int numTaskExecutors;

  /**
   * The number of cores of a MistTask.
   */
  private final int numTaskCores;

  /**
   * The port number of rpc server.
   */
  private final int rpcServerPort;
  @Inject
  private MistTaskConfigs(@Parameter(NumTasks.class) final int numTasks,
                          @Parameter(TaskMemorySize.class) final int taskMemSize,
                          @Parameter(NumExecutors.class) final int numTaskExecutors,
                          @Parameter(NumTaskCores.class) final int numTaskCores,
                          @Parameter(RPCServerPort.class) final int rpcServerPort) {
    this.numTasks = numTasks;
    this.numTaskExecutors = numTaskExecutors;
    this.taskMemSize = taskMemSize;
    this.numTaskCores = numTaskCores;
    this.rpcServerPort = rpcServerPort + 10;
  }

  public int getNumTasks() {
   return numTasks;
  }

  public int getTaskMemSize() {
    return taskMemSize;
  }

  public int getNumTaskExecutors() {
    return numTaskExecutors;
  }

  public int getNumTaskCores() {
    return numTaskCores;
  }

  public int getRpcServerPort() {
    return rpcServerPort;
  }
}
