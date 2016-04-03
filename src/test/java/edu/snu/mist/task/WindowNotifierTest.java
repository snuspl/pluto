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

import edu.snu.mist.api.StreamType;
import edu.snu.mist.task.operators.BaseOperator;
import edu.snu.mist.task.operators.window.WindowedData;
import edu.snu.mist.task.operators.window.WindowOperator;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public final class WindowNotifierTest {

  /**
   * Test whether TimeWindowNotifier gives time notification correctly.
   * This test registers two windowing operators to TimeWindowNotifier
   * and checks whether the two windowing operators receives notification from TimeWindowNotifier.
   * @throws Exception
   */
  @Test
  public void timeWindowNotifierTest() throws Exception {
    final BlockingQueue<Long> timeWindowNotification1 = new LinkedBlockingQueue<>();
    final BlockingQueue<Long> timeWindowNotification2 = new LinkedBlockingQueue<>();
    final StringIdentifierFactory identifierFactory = new StringIdentifierFactory();
    final WindowOperator<Integer, Long> timeWindowOperator1 =
        new TestWindowOperator(timeWindowNotification1, identifierFactory);
    final WindowOperator<Integer, Long> timeWindowOperator2 =
        new TestWindowOperator(timeWindowNotification2, identifierFactory);

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(WindowNotifier.class, TimeWindowNotifier.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());

    try (final WindowNotifier windowNotifier = injector.getInstance(TimeWindowNotifier.class)) {
      windowNotifier.registerWindowOperator(timeWindowOperator1);
      timeWindowNotification1.take();
      windowNotifier.unregisterWindowOperator(timeWindowOperator1);

      windowNotifier.registerWindowOperator(timeWindowOperator2);
      timeWindowNotification2.take();
      timeWindowNotification2.take();
      Assert.assertEquals(0, timeWindowNotification1.size());
    }
  }

  /**
   * Test class for windowing operator.
   */
  class TestWindowOperator extends BaseOperator<Integer, WindowedData<Integer>>
      implements WindowOperator<Integer, Long> {
    private final BlockingQueue queue;

    public TestWindowOperator(final BlockingQueue<Long> queue,
                              final StringIdentifierFactory identifierFactory) {
      super(identifierFactory.getNewInstance("test-query"), identifierFactory.getNewInstance("test-op"));
      this.queue = queue;
    }

    @Override
    public StreamType.OperatorType getOperatorType() {
      return null;
    }

    @Override
    public void windowNotification(final Long notification) {
      queue.add(notification);
    }

    @Override
    public void handle(final Integer input) {

    }
  }
}
