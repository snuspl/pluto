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
package edu.snu.mist.task;

import edu.snu.mist.task.executor.MistExecutor;
import edu.snu.mist.task.parameters.NumExecutors;
import junit.framework.Assert;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

public final class ExecutorListProviderTest {

  /**
   * Test if ExecutorListProviderTest creates correct number of executors.
   * @throws InjectionException
   */
  @Test
  public void executorListProviderTest() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final int numExecutors = 4;
    // Set 4 mist executors
    jcb.bindNamedParameter(NumExecutors.class, numExecutors+"");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final ExecutorListProvider executorListProvider = injector.getInstance(ExecutorListProvider.class);
    final StringIdentifierFactory identifierFactory = injector.getInstance(StringIdentifierFactory.class);
    Assert.assertEquals(numExecutors, executorListProvider.getExecutors().size());

    int i = 0;
    for (final MistExecutor executor : executorListProvider.getExecutors()) {
      Assert.assertEquals("MistExecutor id should be " + "MistExecutor-" + i,
          identifierFactory.getNewInstance("MistExecutor-" + i), executor.getIdentifier());
      i += 1;
    }
  }
}
