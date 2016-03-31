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
package edu.snu.mist.task.executor;

import edu.snu.mist.task.executor.parameters.MistExecutorId;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public final class ThreadExecutorTest {

  @Test(timeout = 5000L)
  public void singleThreadExecutorTest() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(MistExecutorId.class, "singleThreadExecutor");
    jcb.bindImplementation(MistExecutor.class, SingleThreadExecutor.class);

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final MistExecutor executor = injector.getInstance(MistExecutor.class);

    final int numRunnables = 1000;
    final CountDownLatch countDownLatch = new CountDownLatch(numRunnables);
    for (int i = 0; i < numRunnables; i++) {
      executor.submit(() -> countDownLatch.countDown());
    }
    countDownLatch.await();
    executor.close();
  }

  /*
  @Test(timeout = 5000L)
  public void dynamicSingleThreadExecutorTest() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(MistExecutorId.class, "dynamicSingleThreadExecutor");
    jcb.bindImplementation(MistExecutor.class, DynamicSingleThreadExecutor.class);

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final MistExecutor executor = injector.getInstance(MistExecutor.class);

    final int numRunnables = 1000;
    final CountDownLatch countDownLatch = new CountDownLatch(numRunnables);
    for (int i = 0; i < numRunnables; i++) {
      executor.submit(() -> countDownLatch.countDown());
      if (i == numRunnables/2) {
        Thread.sleep(1000);
      }
    }
    countDownLatch.await();
    executor.close();
  }
  */
}
