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
package edu.snu.mist;

import edu.snu.mist.driver.AvroRPCNettyServerWrapper;
import edu.snu.mist.driver.MistDriver;
import edu.snu.mist.driver.SpecificResponderWrapper;
import edu.snu.mist.driver.parameters.*;
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
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Launcher for MistDriver.
 */
public final class MistLauncher {
  private static final Logger LOG = Logger.getLogger(MistLauncher.class.getName());

  /**
   * @return the configuration of the runtime
   */
  private static Configuration getRuntimeConfiguration(final Configuration commandLineConf)
      throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(commandLineConf);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final boolean isLocal = injector.getNamedInstance(IsLocal.class);

    if (isLocal) {
      return LocalRuntimeConfiguration.CONF
          .build();
    } else {
      return YarnClientConfiguration.CONF
          .build();
    }
  }

  /**
   * Gets configurations from command line args.
   */
  private static Configuration getCommandLineConf(final String[] args) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine commandLine = new CommandLine(jcb)
        .registerShortNameOfClass(IsLocal.class)
        .registerShortNameOfClass(NumTaskCores.class)
        .registerShortNameOfClass(NumTasks.class)
        .registerShortNameOfClass(ServerPort.class)
        .registerShortNameOfClass(TaskMemorySize.class);
    commandLine.processCommandLine(args);
    return jcb.build();
  }

  /**
   * @return the configuration of the Mist driver.
   */
  private static Configuration getDriverConfiguration(final Configuration commandLineConf) {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(commandLineConf);
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
   * Start MistLauncher.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) throws Exception {
    final Configuration commandLineConf = getCommandLineConf(args);
    final Configuration runtimeConf = getRuntimeConfiguration(commandLineConf);
    final Configuration driverConf = getDriverConfiguration(commandLineConf);

    final LauncherStatus status = DriverLauncher
        .getLauncher(runtimeConf)
        .run(driverConf);
    LOG.log(Level.INFO, "Mist completed: {0}", status);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private MistLauncher() {
  }
}
