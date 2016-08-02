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

import edu.snu.mist.common.rpc.AvroRPCNettyServerWrapper;
import edu.snu.mist.common.rpc.RPCServerPort;
import edu.snu.mist.driver.parameters.NumTaskCores;
import edu.snu.mist.driver.parameters.NumTasks;
import edu.snu.mist.driver.parameters.TaskMemorySize;
import edu.snu.mist.driver.parameters.TempFolderPath;
import edu.snu.mist.formats.avro.ClientToTaskMessage;
import edu.snu.mist.task.DefaultClientToTaskMessageImpl;
import edu.snu.mist.task.TaskSpecificResponderWrapper;
import edu.snu.mist.task.parameters.NumPeriodicSchedulerThreads;
import edu.snu.mist.task.parameters.NumQueryManagerThreads;
import edu.snu.mist.task.parameters.NumThreads;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * This class contains information for setting MistTasks.
 */
final class MistTaskConfigs {

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
   * The number of threads of a MistTask.
   */
  private final int numTaskThreads;

  /**
   * The number of threads for receiving queries in MistTask.
   */
  private final int numManagerThreads;

  /**
   * The number of cores of a MistTask.
   */
  private final int numTaskCores;

  /**
   * The port number of rpc server of a MistTask.
   */
  private final int rpcServerPort;

  /**
   * Temporary folder path for storing temporay jar files.
   */
  private final String tempFolderPath;

  /**
   * The number of threads for the periodic service scheduler.
   */
  private final int numSchedulerThreads;

  @Inject
  private MistTaskConfigs(@Parameter(NumTasks.class) final int numTasks,
                          @Parameter(TaskMemorySize.class) final int taskMemSize,
                          @Parameter(NumThreads.class) final int numTaskThreads,
                          @Parameter(NumTaskCores.class) final int numTaskCores,
                          @Parameter(RPCServerPort.class) final int rpcServerPort,
                          @Parameter(NumQueryManagerThreads.class) final int numManagerThreads,
                          @Parameter(TempFolderPath.class) final String tempFolderPath,
                          @Parameter(NumPeriodicSchedulerThreads.class) final int numSchedulerThreads) {
    this.numTasks = numTasks;
    this.numTaskThreads = numTaskThreads;
    this.taskMemSize = taskMemSize;
    this.numTaskCores = numTaskCores;
    this.tempFolderPath = tempFolderPath;
    this.numManagerThreads = numManagerThreads;
    this.rpcServerPort = rpcServerPort + 10 > MAX_PORT_NUM ? rpcServerPort - 10 : rpcServerPort + 10;
    this.numSchedulerThreads = numSchedulerThreads;
  }

  public int getNumTasks() {
   return numTasks;
  }

  public int getTaskMemSize() {
    return taskMemSize;
  }

  public int getNumTaskThreads() {
    return numTaskThreads;
  }

  public int getNumTaskCores() {
    return numTaskCores;
  }

  public int getRpcServerPort() {
    return rpcServerPort;
  }

  public String getTempFolderPath() {
    return tempFolderPath;
  }

  public int getNumManagerThreads() {
    return numManagerThreads;
  }

  public int getNumSchedulerThreads() {
    return numSchedulerThreads;
  }

  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

    // Parameter
    jcb.bindNamedParameter(NumTasks.class, Integer.toString(numTasks));
    jcb.bindNamedParameter(TaskMemorySize.class, Integer.toString(taskMemSize));
    jcb.bindNamedParameter(NumThreads.class, Integer.toString(numTaskThreads));
    jcb.bindNamedParameter(NumTaskCores.class, Integer.toString(numTaskCores));
    jcb.bindNamedParameter(RPCServerPort.class, Integer.toString(rpcServerPort));
    jcb.bindNamedParameter(TempFolderPath.class, tempFolderPath);
    jcb.bindNamedParameter(NumQueryManagerThreads.class, Integer.toString(numManagerThreads));
    jcb.bindNamedParameter(NumPeriodicSchedulerThreads.class, Integer.toString(numSchedulerThreads));

    // Implementation
    jcb.bindImplementation(ClientToTaskMessage.class, DefaultClientToTaskMessageImpl.class);
    jcb.bindConstructor(Server.class, AvroRPCNettyServerWrapper.class);
    jcb.bindConstructor(SpecificResponder.class, TaskSpecificResponderWrapper.class);
    return jcb.build();
  }
}
