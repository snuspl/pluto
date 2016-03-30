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
}
