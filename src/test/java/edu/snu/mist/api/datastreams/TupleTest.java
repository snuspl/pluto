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
package edu.snu.mist.api.datastreams;

import edu.snu.mist.common.types.Tuple;
import edu.snu.mist.common.types.Tuple2;
import org.junit.Assert;
import org.junit.Test;

/**
 * The test class for MIST Tuple interface.
 */
public class TupleTest {

  private final Tuple tuple = new Tuple2<>("a", 1);

  /**
   * Test for Tuple getValue() method.
   */
  @Test
  public void testTupleGetValue() {
    Assert.assertEquals(tuple.get(0), "a");
    Assert.assertEquals(tuple.get(1), 1);
  }

  /**
   * Test for the invalid field number handling of getValue() method.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testTupleGetValueException() {
    tuple.get(2);
  }

  /**
   * Test for Tuple getFieldType() method.
   */
  @Test
  public void testTupleGetFieldType() {
    Assert.assertEquals(tuple.getFieldType(0), String.class);
    Assert.assertEquals(tuple.getFieldType(1), Integer.class);
  }

  /**
   * Test for the invalid field number handling of getFieldType() method.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testTupleGetFieldTypeException() {
    tuple.getFieldType(2);
  }
}