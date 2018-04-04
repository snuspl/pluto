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

import edu.snu.mist.core.MistLauncher;
import edu.snu.mist.core.parameters.*;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.DefaultNumEventProcessors;
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
   * @throws InjectionException
   */
  @Test
  public void testLaunchDriverMIST() throws InjectionException {
    launchDriverTestHelper();
  }

  /**
   * Test whether MistDriver runs MistTaks successfully.
   * @throws InjectionException
   */
  public void launchDriverTestHelper() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DriverRuntimeType.class, "LOCAL");
    jcb.bindNamedParameter(NumTaskCores.class, "1");
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "1");
    jcb.bindNamedParameter(TaskMemorySize.class, "256");
    jcb.bindNamedParameter(NumMasterCores.class, "1");
    jcb.bindNamedParameter(MasterMemorySize.class, "256");

    final Configuration runtimeConf = LocalRuntimeConfiguration.CONF
        .build();
    final Configuration driverConf = MistLauncher.getDriverConfiguration(jcb.build());

    final LauncherStatus state = TestLauncher.run(runtimeConf, driverConf, 4000);
    final Optional<Throwable> err = state.getError();
    System.out.println("Job state after execution: " + state);
    System.out.println("Error: " + err.get());
    Assert.assertFalse(err.isPresent());
  }
}
