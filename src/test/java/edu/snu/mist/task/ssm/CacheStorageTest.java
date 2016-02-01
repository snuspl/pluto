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

package edu.snu.mist.task.ssm;

import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.Identifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CacheStorageTest {

  private final Identifier qid1 = new StringIdentifierFactory().getNewInstance("qid1");
  private final Identifier qid2 = new StringIdentifierFactory().getNewInstance("qid2");
  private final Identifier oid1 = new StringIdentifierFactory().getNewInstance("oid1");
  private final Identifier oid2 = new StringIdentifierFactory().getNewInstance("oid2");
  private final Identifier oid3 = new StringIdentifierFactory().getNewInstance("oid3");
  private final OperatorState<Integer> value1 = new OperatorState<>(1);
  private final OperatorState<Integer> value2 = new OperatorState<>(2);

  /**
   * Tests whether the create method in the CacheStorage correctly creates queryId - queryState pairs.
   */
  @Test
  public void createCacheStorageTest() {
    final CacheStorage cache = new CacheStorageImpl();
    final Map<Identifier, OperatorState> queryState1 = new HashMap<>();
    final Map<Identifier, OperatorState> queryState2 = new HashMap<>();

    queryState1.put(oid1, value1);
    queryState2.put(oid1, value2);

    //Test if the state was created well.
    Assert.assertEquals(true, cache.create(qid1, queryState1));

    //Test if create fails when the same queryId is requested to be created again.
    Assert.assertEquals(false, cache.create(qid1, queryState2));

    //Test if create works if a different queryId with the same queryState is created.
    Assert.assertEquals(true, cache.create(qid2, queryState1));
  }

  /**
   * Tests whether the read method in the CacheStorage returns the correct values.
   */
  @Test
  public void readCacheStorageTest() {
    final CacheStorage cache = new CacheStorageImpl();
    final Map<Identifier, OperatorState> queryState1 = new HashMap<>();

    queryState1.put(oid1, value1);
    queryState1.put(oid2, value2);

    cache.create(qid1, queryState1);

    //Test if read works if queryId and operatorId is present in the cache.
    Assert.assertEquals(value1, cache.read(qid1, oid1));
    Assert.assertEquals(value2, cache.read(qid1, oid2));

    //Test if read is unsuccessful if the queryId is in the cache but the operatorId is not in the queryState.
    Assert.assertSame(null, cache.read(qid1, oid3));

    //Test if read is unsuccessful if the queryId is not in the cache but the operatorId is in a certain queryState.
    Assert.assertSame(null, cache.read(qid2, oid1));

    //Test if read is unsuccessful if both the queryId and the operatorId is not in the cache.
    Assert.assertSame(null, cache.read(qid2, oid3));
  }

  /**
   * Tests whether the update method in the CacheStorage correctly updates the states.
   */
  @Test
  public void updateCacheStorageTest() {
    final CacheStorage cache = new CacheStorageImpl();
    final Map<Identifier, OperatorState> queryState1 = new HashMap<>();

    queryState1.put(oid1, value1);
    final OperatorState<Integer> value3 = new OperatorState<>(10);
    final OperatorState<String> value4 = new OperatorState<>("abcd");

    cache.create(qid1, queryState1);

    //Test if update works when there is the queryId and operatorId is present in the cache.
    Assert.assertEquals(value1, cache.read(qid1, oid1));
    Assert.assertEquals(true, cache.update(qid1, oid1, value3));
    Assert.assertEquals(value3, cache.read(qid1, oid1));

    //Test if update works when another type of state is updated into the cache.
    Assert.assertEquals(true, cache.update(qid1, oid1, value4));
    Assert.assertEquals(value4, cache.read(qid1, oid1));
    Assert.assertEquals("abcd", cache.read(qid1, oid1).getState());

    //Test if update works when the same state is re-assigned.
    Assert.assertEquals(true, cache.update(qid1, oid1, value4));
    Assert.assertEquals(value4, cache.read(qid1, oid1));

    //Test if update works when the queryId is present but operatorId is not.
    Assert.assertEquals(true, cache.update(qid1, oid2, value3));
    Assert.assertSame(value3, cache.read(qid1, oid2));

    //Test if update returns false when the queryId is not present but operatorId is.
    Assert.assertEquals(false, cache.update(qid2, oid1, value3));

    //Test if update returns false when both the queryId and operatorId is not present in the cache.
    Assert.assertEquals(false, cache.update(qid2, oid3, value3));
  }

  /**
   * Test if the delete method in the CacheStorage correctly deletes the queryState of the queryId.
   */
  @Test
  public void deleteCacheStorageTest() {
    final CacheStorage cache = new CacheStorageImpl();
    final Map<Identifier, OperatorState> queryState1 = new HashMap<>();

    queryState1.put(oid1, value1);
    cache.create(qid1, queryState1);

    //Test if deletion deletes the entire queryState
    Assert.assertEquals(value1, cache.read(qid1, oid1));
    Assert.assertEquals(true, cache.delete(qid1));
    Assert.assertSame(null, cache.read(qid1, oid1));

    //Test if deletion returns false if deleting on a non-existing queryId.
    Assert.assertEquals(false, cache.delete(qid1));
    Assert.assertEquals(false, cache.delete(qid2));
  }

  /**
   * Test if create works on a deleted queryId.
   */
  @Test
  public void deleteCreateCacheStorageTest() {
    final CacheStorage cache = new CacheStorageImpl();
    final Map<Identifier, OperatorState> queryState1 = new HashMap<>();
    final Map<Identifier, OperatorState> queryState2 = new HashMap<>();

    queryState1.put(oid1, value1);
    queryState2.put(oid1, value2);
    cache.create(qid1, queryState1);

    Assert.assertEquals(value1, cache.read(qid1, oid1));
    Assert.assertEquals(true, cache.delete(qid1));
    Assert.assertEquals(true, cache.create(qid1, queryState2));
    Assert.assertEquals(value2, cache.read(qid1, oid1));
  }

  /**
   * Test if update works on a deleted queryId.
   */
  @Test
  public void deleteUpdateCacheStorageTest() {
    final CacheStorage cache = new CacheStorageImpl();
    final Map<Identifier, OperatorState> queryState1 = new HashMap<>();

    queryState1.put(oid1, value1);
    cache.create(qid1, queryState1);

    Assert.assertEquals(value1, cache.read(qid1, oid1));
    Assert.assertEquals(true, cache.delete(qid1));
    Assert.assertEquals(false, cache.update(qid1, oid1, value2));
  }
}
