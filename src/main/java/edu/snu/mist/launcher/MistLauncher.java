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
package edu.snu.mist.launcher;

import edu.snu.mist.common.rpc.AvroRPCNettyServerWrapper;
import edu.snu.mist.common.rpc.RPCServerPort;
import edu.snu.mist.driver.MistDriver;
import edu.snu.mist.driver.SpecificResponderWrapper;
import edu.snu.mist.driver.parameters.NumTaskCores;
import edu.snu.mist.driver.parameters.NumTasks;
import edu.snu.mist.driver.parameters.TaskMemorySize;
import edu.snu.mist.task.parameters.NumExecutors;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverImpl;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Launcher for MistDriver.
 */
public final class MistLauncher {
  private static final Logger LOG = Logger.getLogger(MistLauncher.class.getName());

  /**
   * Enum for runtime type.
   */
  public enum RuntimeType {
    LOCAL,
    YARN;
  }

  /**
   * Runtime Configuration.
   */
  private final Configuration mistRuntimeConf;

  /**
   * Timeout for the driver.
   */
  private int timeOut = 0;

  @Inject
  private MistLauncher(@Parameter(DriverRuntimeType.class) final String runtimeType) {
    this.mistRuntimeConf = getRuntimeConfiguration(MistLauncher.RuntimeType.valueOf(runtimeType));
  }

  /**
   * @return the configuration of the runtime
   */
  private Configuration getRuntimeConfiguration(final RuntimeType runtimeType) {
    switch (runtimeType) {
      case LOCAL:
        return LocalRuntimeConfiguration.CONF.build();
      case YARN:
        return YarnClientConfiguration.CONF.build();
      default:
        throw new IllegalArgumentException(runtimeType.name() + " Runtime Type is not supported yet.");
    }
  }

  /**
   * @return the configuration of the Mist driver.
   */
  private static Configuration getDriverConfiguration(final Configuration conf) {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(conf);
    jcb.bindImplementation(NameResolver.class, LocalNameResolverImpl.class);
    jcb.bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class);
    jcb.bindConstructor(SpecificResponder.class, SpecificResponderWrapper.class);
    jcb.bindConstructor(Server.class, AvroRPCNettyServerWrapper.class);

    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(MistDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "MistDriver")
        .set(DriverConfiguration.ON_DRIVER_STARTED, MistDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, MistDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, MistDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, MistDriver.RunningTaskHandler.class)
        .build();

    return Configurations.merge(driverConf, jcb.build());
  }

  /**
   * Instantiate a launcher for the given Configuration.
   *
   * @param runtimeType whether the driver run locally or in Yarn executor
   * @return a MistLauncher based on the given option
   * @throws InjectionException on configuration errors
   */
  public static MistLauncher getLauncher(final RuntimeType runtimeType) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DriverRuntimeType.class, runtimeType.name());
    final Configuration mistRuntimeConf = jcb.build();
    return Tang.Factory.getTang()
        .newInjector(mistRuntimeConf)
        .getInstance(MistLauncher.class);
  }

  /**
   * Set the timeout for the driver.
   * @param timeout timeout on the job.
   * @return this MistLauncher which contains the timeout.
   */
  public MistLauncher setTimeout(final int timeout) {
    this.timeOut = timeout;
    return this;
  }

  /**
   * Run the Mist Driver.
   * @param numTaskCores the number of cores for tasks
   * @param numExecutors the number of executors
   * @param numTasks the number of tasks
   * @param rpcServerPort the RPC Server Port
   * @param taskMemorySize the Memory size of the task
   * @return a status of the driver
   */
  public LauncherStatus run(final int numTaskCores, final int numExecutors, final int numTasks,
                            final int rpcServerPort, final int taskMemorySize) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(NumTaskCores.class, Integer.toString(numTaskCores));
    jcb.bindNamedParameter(NumExecutors.class, Integer.toString(numExecutors));
    jcb.bindNamedParameter(NumTasks.class, Integer.toString(numTasks));
    jcb.bindNamedParameter(RPCServerPort.class, Integer.toString(rpcServerPort));
    jcb.bindNamedParameter(TaskMemorySize.class, Integer.toString(taskMemorySize));
    final Configuration driverConf = getDriverConfiguration(jcb.build());

    final DriverLauncher launcher =  DriverLauncher.getLauncher(mistRuntimeConf);
    final LauncherStatus status = timeOut == 0 ? launcher.run(driverConf) : launcher.run(driverConf, timeOut);

    LOG.log(Level.INFO, "Mist completed: {0}", status);

    return status;
  }
}
