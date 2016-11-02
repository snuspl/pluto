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
package edu.snu.mist.core.task;

import edu.snu.mist.core.parameters.NumThreads;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.Set;

public class ThreadManagerTest {

  /**
   * Test whether FixThreadManager creates fixed number of threads correctly.
   */
  @Test
  public void fixedThreadManagerTest() throws Exception {
    final int numThreads = 5;
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(NumThreads.class, Integer.toString(numThreads));

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final FixedThreadManager threadManager = injector.getInstance(FixedThreadManager.class);
    final Set<Thread> threads = threadManager.getThreads();
    Assert.assertEquals(threads.size(), numThreads);

    threadManager.close();
  }
}
