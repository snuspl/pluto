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
package edu.snu.mist.core.task.globalsched;

import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.globalsched.metrics.NumGroupsMetricEventHandler;
import edu.snu.mist.core.task.metrics.MetricHolder;
import edu.snu.mist.core.task.metrics.MetricTrackEvent;
import edu.snu.mist.core.task.metrics.NormalMetric;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * Test whether NumGroupsMetricEventHandler tracks the metrics properly or not.
 */
public final class NumGroupsMetricEventHandlerTest {

  private MistPubSubEventHandler metricPubSubEventHandler;
  private GlobalSchedGroupInfoMap groupInfoMap;
  private MetricHolder metricHolder;
  private NumGroupsMetricEventHandler handler;
  private static final int UPDATE_GROUP_SIZE = 10;
  private static final int DEFAULT_GROUP_SIZE = 0;

  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    metricHolder = injector.getInstance(MetricHolder.class);
    metricHolder.putNormalMetric(
        MetricHolder.NormalMetricType.NUM_GROUP, new NormalMetric<>(DEFAULT_GROUP_SIZE));
    groupInfoMap = injector.getInstance(GlobalSchedGroupInfoMap.class);
    metricPubSubEventHandler = injector.getInstance(MistPubSubEventHandler.class);
    handler = injector.getInstance(NumGroupsMetricEventHandler.class);
  }

  /**
   * Test that a metric track event handler can track the total number of groups properly.
   */
  @Test(timeout = 1000L)
  public void testNumGroupsMetricTracking() throws Exception {
    // Test default value
    Assert.assertEquals(DEFAULT_GROUP_SIZE,
        metricHolder.getNormalMetric(MetricHolder.NormalMetricType.NUM_GROUP).getValue());

    // Update the value
    for (int i = 0; i < UPDATE_GROUP_SIZE; i++) {
      final GlobalSchedGroupInfo groupInfo = mock(GlobalSchedGroupInfo.class);
      groupInfoMap.put(String.valueOf(i), groupInfo);
    }

    // Wait the tracker to call handler
    metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricTrackEvent());
    Assert.assertEquals(
        UPDATE_GROUP_SIZE, metricHolder.getNormalMetric(MetricHolder.NormalMetricType.NUM_GROUP).getValue());
  }
}
