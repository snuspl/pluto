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
package edu.snu.mist.core;

import edu.snu.mist.common.rpc.AvroRPCNettyServerWrapper;
import edu.snu.mist.common.rpc.RPCServerPort;
import edu.snu.mist.core.driver.MistDriver;
import edu.snu.mist.core.driver.SpecificResponderWrapper;
import edu.snu.mist.core.parameters.DriverRuntimeType;
import edu.snu.mist.core.parameters.NumTaskCores;
import edu.snu.mist.core.parameters.NumTasks;
import edu.snu.mist.core.parameters.TaskMemorySize;
import edu.snu.mist.core.task.groupAware.eventProcessors.parameters.DefaultNumEventProcessors;
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
    YARN
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
    try {
      this.mistRuntimeConf = getRuntimeConfiguration(MistLauncher.RuntimeType.valueOf(runtimeType));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(runtimeType + " Runtime Type is not supported yet.");
    }
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
  public static Configuration getDriverConfiguration(final Configuration conf) {
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
   * Instantiate a launcher for the given option.
   *
   * @param runtimeType whether the driver run locally or in Yarn executor
   * @return a MistLauncher based on the given option
   * @throws InjectionException on configuration errors
   */
  public static MistLauncher getLauncher(final RuntimeType runtimeType) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DriverRuntimeType.class, runtimeType.name());
    final Configuration mistRuntimeConf = jcb.build();
    return getLauncherFromConf(mistRuntimeConf);
  }

  /**
   * Instantiate a launcher for the given Configuration.
   * @param runtimeConf Configuration for the MistLauncher
   * @return a MistLauncher based on the given option
   * @throws InjectionException on configuration errors
   */
  public static MistLauncher getLauncherFromConf(final Configuration runtimeConf) throws InjectionException {
    return Tang.Factory.getTang()
        .newInjector(runtimeConf)
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
   * Run the Mist Driver for the given options.
   * @param numTaskCores the number of cores for tasks
   * @param numEventProcessors the number of event processors
   * @param numTasks the number of tasks
   * @param rpcServerPort the RPC Server Port
   * @param taskMemorySize the Memory size of the task
   * @return a status of the driver
   * @throws InjectionException on configuration errors
   */
  public LauncherStatus run(final int numTaskCores, final int numEventProcessors, final int numTasks,
                            final int rpcServerPort, final int taskMemorySize) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(NumTaskCores.class, Integer.toString(numTaskCores));
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, Integer.toString(numEventProcessors));
    jcb.bindNamedParameter(NumTasks.class, Integer.toString(numTasks));
    jcb.bindNamedParameter(RPCServerPort.class, Integer.toString(rpcServerPort));
    jcb.bindNamedParameter(TaskMemorySize.class, Integer.toString(taskMemorySize));

    return runFromConf(jcb.build());
  }

  /**
   * Run the Mist Driver for the given Configuration.
   * @param driverConf The Configuration for the driver
   * @return a status of the driver
   * @throws InjectionException on configuration errors
   */
  public LauncherStatus runFromConf(final Configuration driverConf) throws InjectionException {
    final DriverLauncher launcher = DriverLauncher.getLauncher(mistRuntimeConf);
    final Configuration mistDriverConf = getDriverConfiguration(driverConf);
    final LauncherStatus status = timeOut == 0 ? launcher.run(mistDriverConf) : launcher.run(mistDriverConf, timeOut);

    LOG.log(Level.INFO, "Mist completed: {0}", status);

    return status;
  }
}
