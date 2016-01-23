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
import edu.snu.mist.driver.parameters.IsLocal;
import edu.snu.mist.driver.parameters.NumTaskCores;
import edu.snu.mist.driver.parameters.TaskMemorySize;
import edu.snu.mist.task.parameters.NumExecutors;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverImpl;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.IdentifierFactory;
import org.junit.Assert;
import org.junit.Test;

public final class MistDriverTest {

  /**
   * Test whether MistDriver runs successfully.
   * @throws InjectionException
   */
  @Test
  public void launchDriverTest() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(NameResolver.class, LocalNameResolverImpl.class);
    jcb.bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class);
    jcb.bindConstructor(SpecificResponder.class, SpecificResponderWrapper.class);
    jcb.bindConstructor(Server.class, AvroRPCNettyServerWrapper.class);
    jcb.bindNamedParameter(IsLocal.class, "true");
    jcb.bindNamedParameter(NumTaskCores.class, "1");
    jcb.bindNamedParameter(NumExecutors.class, "1");
    jcb.bindNamedParameter(TaskMemorySize.class, "256");

    final Configuration runtimeConf = LocalRuntimeConfiguration.CONF
        .build();

    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(MistDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "MistDriverTest")
        .set(DriverConfiguration.ON_DRIVER_STARTED, MistDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, MistDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, MistDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, MistDriver.RunningTaskHandler.class)
        .build();

    final LauncherStatus state = TestLauncher.run(runtimeConf, Configurations
        .merge(jcb.build(), driverConf), 5000);
    final Optional<Throwable> err = state.getError();
    System.out.println("Job state after execution: " + state);
    System.out.println("Error: " + err.get());
    Assert.assertFalse(err.isPresent());
  }
}
