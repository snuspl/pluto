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
package edu.snu.mist.core.task;

import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import edu.snu.mist.utils.TestOperator;

public class HeadOperatorManagerTest {

  /**
   * Test whether RandomlyPickManager selects a head operator.
   */
  @Test
  public void randomPickManagerTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final RandomlyPickManager headOperatorManager = injector.getInstance(RandomlyPickManager.class);

    // Select a query
    final PhysicalOperator operator = new DefaultPhysicalOperator(
        new TestOperator("op1"), true, null);
    headOperatorManager.insert(operator);
    final PhysicalOperator selectedOperator = headOperatorManager.pickHeadOperator();
    Assert.assertEquals(operator, selectedOperator);

    // When HeadOperatorManager has no operator, it should returns null
    headOperatorManager.delete(operator);
    final PhysicalOperator op = headOperatorManager.pickHeadOperator();
    Assert.assertEquals(null, op);
  }
}
