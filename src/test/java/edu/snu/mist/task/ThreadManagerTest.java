package edu.snu.mist.task;

import edu.snu.mist.task.parameters.NumExecutors;
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
    jcb.bindNamedParameter(NumExecutors.class, Integer.toString(numThreads));

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final FixedThreadManager threadManager = injector.getInstance(FixedThreadManager.class);
    final Set<Thread> threads = threadManager.getThreads();
    Assert.assertEquals(threads.size(), numThreads);

    threadManager.close();
  }
}
