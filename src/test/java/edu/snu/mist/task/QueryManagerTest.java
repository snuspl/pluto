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

import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

public class QueryManagerTest {

  /**
   * Test whether RandomlyPickManager selects a query.
   */
  @Test
  public void randomPickManagerTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final RandomlyPickManager queryManager = injector.getInstance(RandomlyPickManager.class);

    // Select a query
    final PartitionedQuery query = new DefaultPartitionedQuery();
    queryManager.insert(query);
    final PartitionedQuery selectedQuery = queryManager.pickQuery();
    Assert.assertEquals(query, selectedQuery);

    // When QueryManager has no query, it should returns null
    queryManager.delete(query);
    final PartitionedQuery q = queryManager.pickQuery();
    Assert.assertEquals(null, q);
  }
}
