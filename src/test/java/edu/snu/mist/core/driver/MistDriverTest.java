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

import edu.snu.mist.common.rpc.RPCServerPort;
import edu.snu.mist.core.MistLauncher;
import edu.snu.mist.core.driver.parameters.ExecutionModelOption;
import edu.snu.mist.core.parameters.DriverRuntimeType;
import edu.snu.mist.core.parameters.NumTaskCores;
import edu.snu.mist.core.parameters.TaskMemorySize;
import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.globalsched.parameters.GroupSchedModelType;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public final class MistDriverTest {

  /**
   * Test whether MistDriver runs the task of option1 successfully.
   * @throws InjectionException
   */
  @Test
  public void testLaunchDriverOption1() throws InjectionException {
    launchDriverTestHelper(1, 20332, "none");
  }

  /**
   * Test whether MistDriver runs the task of group scheduling (blocking) successfully.
   * @throws InjectionException
   */
  @Test
  public void testLaunchDriverOption2Blocking() throws InjectionException {
    launchDriverTestHelper(2, 20333, "blocking");
  }

  /**
   * Test whether MistDriver runs the task of group scheduling (nonblocking) successfully.
   * @throws InjectionException
   */
  @Test
  public void testLaunchDriverOption2NonBlocking() throws InjectionException {
    launchDriverTestHelper(2, 20335, "nonblocking");
  }

  /**
   * Test whether MistDriver runs the task of group scheduling (activation with polling) successfully.
   * @throws InjectionException
   */
  @Test
  public void testLaunchDriverOption2Polling() throws InjectionException {
    launchDriverTestHelper(2, 20336, "polling");
  }

  /**
   * Test whether MistDriver runs the task of option3 (thread-based model) successfully.
   * @throws InjectionException
   */
  @Test
  public void testLaunchDriverOption3() throws InjectionException {
    launchDriverTestHelper(3, 20334, "none");
  }

  /**
   * Test whether MistDriver runs MistTaks successfully.
   * @throws InjectionException
   */
  public void launchDriverTestHelper(final int executionModelOption,
                                     final int rpcServerPort,
                                     final String groupSchedModel) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DriverRuntimeType.class, "LOCAL");
    jcb.bindNamedParameter(NumTaskCores.class, "1");
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "1");
    jcb.bindNamedParameter(TaskMemorySize.class, "256");
    jcb.bindNamedParameter(RPCServerPort.class, Integer.toString(rpcServerPort));
    jcb.bindNamedParameter(ExecutionModelOption.class, Integer.toString(executionModelOption));
    jcb.bindNamedParameter(GroupSchedModelType.class, groupSchedModel);

    final Configuration runtimeConf = LocalRuntimeConfiguration.CONF
        .build();
    final Configuration driverConf = MistLauncher.getDriverConfiguration(jcb.build());

    final LauncherStatus state = TestLauncher.run(runtimeConf, driverConf, 5000);
    final Optional<Throwable> err = state.getError();
    System.out.println("Job state after execution: " + state);
    System.out.println("Error: " + err.get());
    Assert.assertFalse(err.isPresent());
  }
}
