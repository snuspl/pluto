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
package edu.snu.mist.core.task.metrics;

import org.junit.Test;

import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Test whether CpuUtilMetricEventHandler tracks the CPU utilization properly or not.
 */
public final class CpuUtilMetricEventHandlerTest {

  /**
   * Test that MBean cpu tracking is supported by current JVM.
   */
  @Test(timeout = 2000L)
  public void testCpuUtilMetricTracking() throws Exception {

    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    final ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
    final AttributeList list = mbs.getAttributes(name, new String[]{"SystemCpuLoad", "ProcessCpuLoad"});
  }
}
