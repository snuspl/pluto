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
package edu.snu.mist.task.operators.window;

import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.operators.parameters.OperatorId;
import edu.snu.mist.task.operators.parameters.TimeWindowInterval;
import edu.snu.mist.task.operators.parameters.TimeWindowSize;
import edu.snu.mist.task.operators.parameters.TimeWindowStartTime;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class WindowOperatorTest {

  /**
   * Test whether TimeWindowOperator emits outputs correctly.
   * When window size is 4 and interval is 2, TimeWindowOperator should emit outputs every 2 seconds.
   * When window size is 5 and interval is 3, TimeWindowOperator should emit outputs every 3 seconds.
   * @throws InjectionException
   */
  @Test
  public void testTimeWindowOperator() throws InjectionException {
    // Test when window size = 4 (sec) and interval = 2 (sec)
    final int windowSize = 4;
    final int windowInterval = 2;
    // Notification times
    final List<Long> notificationTimes = Arrays.asList(
        TimeUnit.SECONDS.toMillis(1), TimeUnit.SECONDS.toMillis(2),
        TimeUnit.SECONDS.toMillis(3), TimeUnit.SECONDS.toMillis(4),
        TimeUnit.SECONDS.toMillis(5), TimeUnit.SECONDS.toMillis(6),
        TimeUnit.SECONDS.toMillis(7), TimeUnit.SECONDS.toMillis(8));
    // Inputs.
    // Null represents no input.
    final List<List<Integer>> inputs = new LinkedList<>();
    inputs.add(Arrays.asList(1, 2, 3));
    inputs.add(Arrays.asList(4, 5, 6));
    inputs.add(Arrays.asList(7, 8, 9));
    inputs.add(Arrays.asList(10, 11, 12));
    inputs.add(null);
    inputs.add(null);
    inputs.add(null);
    inputs.add(Arrays.asList(1));
    // Expected outputs.
    // Null represents no output.
    final List<List<Integer>> outputs = new LinkedList<>();
    outputs.add(null);
    outputs.add(Arrays.asList(1, 2, 3, 4, 5, 6));
    outputs.add(null);
    outputs.add(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12));
    outputs.add(null);
    outputs.add(Arrays.asList(7, 8, 9, 10, 11, 12));
    outputs.add(null);
    outputs.add(Arrays.asList(1));
    // Test
    testHelper(windowSize, windowInterval, 0L, notificationTimes, inputs, outputs);

    // Test when window size = 5 (sec) and interval = 3 (sec)
    final int windowSize2 = 5;
    final int windowInterval2 = 3;
    // Notification times
    final List<Long> notificationTimes2 = Arrays.asList(
        TimeUnit.SECONDS.toMillis(1), TimeUnit.SECONDS.toMillis(2),
        TimeUnit.SECONDS.toMillis(3), TimeUnit.SECONDS.toMillis(4),
        TimeUnit.SECONDS.toMillis(5), TimeUnit.SECONDS.toMillis(6),
        TimeUnit.SECONDS.toMillis(7), TimeUnit.SECONDS.toMillis(8),
        TimeUnit.SECONDS.toMillis(9), TimeUnit.SECONDS.toMillis(10),
        TimeUnit.SECONDS.toMillis(11), TimeUnit.SECONDS.toMillis(12));
    // Inputs.
    // Null represents no input.
    final List<List<Integer>> inputs2 = new LinkedList<>();
    inputs2.add(Arrays.asList(1, 2));
    inputs2.add(Arrays.asList(3, 4));
    inputs2.add(Arrays.asList(5, 6));
    inputs2.add(null);
    inputs2.add(null);
    inputs2.add(Arrays.asList(7, 8));
    inputs2.add(Arrays.asList(3, 4));
    inputs2.add(Arrays.asList(5, 6));
    inputs2.add(null);
    inputs2.add(null);
    inputs2.add(null);
    inputs2.add(null);
    // Expected outputs.
    // Null represents no output.
    final List<List<Integer>> outputs2 = new LinkedList<>();
    outputs2.add(null);
    outputs2.add(null);
    outputs2.add(Arrays.asList(1, 2, 3, 4, 5, 6));
    outputs2.add(null);
    outputs2.add(null);
    outputs2.add(Arrays.asList(3, 4, 5, 6, 7, 8));
    outputs2.add(null);
    outputs2.add(null);
    outputs2.add(Arrays.asList(7, 8, 3, 4, 5, 6));
    outputs2.add(null);
    outputs2.add(null);
    outputs2.add(Arrays.asList(5, 6));
    // Test
    testHelper(windowSize2, windowInterval2, 0L, notificationTimes2, inputs2, outputs2);
  }

  /**
   * This is a helper method for testing windowing operator.
   * It feeds inputs every notification times,
   * checks outputs when the emitted outputs are equal to the expected outputs.
   * @param timeWindowSize time window size
   * @param timeWindowInterval time window interval
   * @param startTime start time of windowing operator
   * @param notificationTimes notification times
   * @param inputs inputs
   * @param outputs outputs
   * @throws InjectionException
   */
  private void testHelper(final int timeWindowSize,
                         final int timeWindowInterval,
                         final long startTime,
                         final List<Long> notificationTimes,
                         final List<List<Integer>> inputs,
                         final List<List<Integer>> outputs) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(OperatorId.class, "testTimeWindowingOp");
    jcb.bindNamedParameter(QueryId.class, "testQuery");
    jcb.bindNamedParameter(TimeWindowInterval.class, Integer.toString(timeWindowInterval));
    jcb.bindNamedParameter(TimeWindowSize.class, Integer.toString(timeWindowSize));
    jcb.bindNamedParameter(TimeWindowStartTime.class, Long.toString(startTime));

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final List<List<Integer>> results = new LinkedList<>();
    final TimeWindowOperator<Integer> timeWindowOperator =
        injector.getInstance(TimeWindowOperator.class);
    timeWindowOperator.setOutputEmitter(output -> results.add(output.getData()));

    int i = 0;
    while (i < notificationTimes.size()) {
      final long notificationTime = notificationTimes.get(i);
      final List<Integer> inputList = inputs.get(i);
      final List<Integer> outputList = outputs.get(i);
      if (inputList != null) {
        for (final Integer input : inputList) {
          timeWindowOperator.handle(input);
        }
      }
      timeWindowOperator.windowNotification(notificationTime);
      if (outputList != null) {
        Assert.assertEquals("WindowOperator should emit output when time=" + notificationTime,
            1, results.size());
        Assert.assertEquals("Output should be " + outputList, outputList, results.remove(0));
      } else {
        Assert.assertEquals("WindowOperator should not emit output when time=" + notificationTime,
            0, results.size());
      }
      i += 1;
    }
  }
}
