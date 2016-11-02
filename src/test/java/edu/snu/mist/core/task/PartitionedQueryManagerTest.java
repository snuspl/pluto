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

import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

public class PartitionedQueryManagerTest {

  /**
   * Test whether RandomlyPickManager selects a query.
   */
  @Test
  public void randomPickManagerTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final RandomlyPickManager partitionedQueryManager = injector.getInstance(RandomlyPickManager.class);

    // Select a query
    final PartitionedQuery query = new DefaultPartitionedQuery();
    partitionedQueryManager.insert(query);
    final PartitionedQuery selectedQuery = partitionedQueryManager.pickQuery();
    Assert.assertEquals(query, selectedQuery);

    // When QueryPickManager has no query, it should returns null
    partitionedQueryManager.delete(query);
    final PartitionedQuery q = partitionedQueryManager.pickQuery();
    Assert.assertEquals(null, q);
  }
}
