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

package edu.snu.mist.task.ssm.orientdb;

import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;
import org.apache.reef.wake.Identifier;

public class SSMImplOrientDbTest {
  /**
   * Tests OrientDB on whether it retrieves 'null' if the key was not stored in the database.
   * @throws InjectionException
   */
  @Test
  public void orientGetEmptyTest() throws InjectionException {
    //open orient db on a new path
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(OrientDbPath.class, "plocal:/tmp/SSM-orientdbTest");
    injector.bindVolatileParameter(OrientDbDropDb.class, true);
    injector.bindVolatileParameter(OrientDbSize.class, 100);
    SSMImplOrientDb orientSSM = injector.getInstance(SSMImplOrientDb.class);

    Identifier id1 = new StringIdentifierFactory().getNewInstance("Operator1");

    Assert.assertSame(null, orientSSM.get(id1));
  }

  /**
   * Tests OrientDB on whether it can store, replace and retrieve values correctly.
   * @throws InjectionException
   */
  @Test
  public void orientSetGetTest() throws InjectionException {
    //open orient db on a new path
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(OrientDbPath.class, "plocal:/tmp/SSM-orientdbTest");
    injector.bindVolatileParameter(OrientDbDropDb.class, true);
    injector.bindVolatileParameter(OrientDbSize.class, 100);
    SSMImplOrientDb orientSSM = injector.getInstance(SSMImplOrientDb.class);

    Identifier id1 = new StringIdentifierFactory().getNewInstance("Operator1");
    Identifier id2 = new StringIdentifierFactory().getNewInstance("Operator2");

    Integer value1 = 1;
    Integer value2 = 2;
    Character value3= 'c';

    //Check if 'set' saves the right value
    orientSSM.set(id1, value1);
    Assert.assertEquals(value1, orientSSM.get(id1));

    //Check what happens when the value is replaced (on the same key)
    orientSSM.set(id1, value2);
    Assert.assertEquals(value2, orientSSM.get(id1));

    //Check if different types can be saved
    orientSSM.set(id2, value2);
    Assert.assertEquals(value2, orientSSM.get(id2));
    orientSSM.set(id2, value3);
    Assert.assertEquals(value3, orientSSM.get(id2));
  }

  /**
   * Tests OrientDB on whether it can delete the key-value pair from the database.
   * @throws InjectionException
   */
  @Test
  public void orientDeleteTest() throws InjectionException {
    //open orient db on a new path
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(OrientDbPath.class, "plocal:/tmp/SSM-orientdbTest");
    injector.bindVolatileParameter(OrientDbDropDb.class, true);
    injector.bindVolatileParameter(OrientDbSize.class, 100);
    SSMImplOrientDb orientSSM = injector.getInstance(SSMImplOrientDb.class);

    Identifier id1 = new StringIdentifierFactory().getNewInstance("Operator1");
    Identifier id2 = new StringIdentifierFactory().getNewInstance("Operator2");

    Integer value1 = 1;
    orientSSM.set(id1, value1);

    //Check whether deletion returned true.
    Assert.assertEquals(true, orientSSM.delete(id1));

    //Check whether the deleted identifier is present in the database.
    Assert.assertSame(null, orientSSM.get(id1));
    Assert.assertEquals(false, orientSSM.delete(id1));

    //Check whether deletion on a non-existing key does not work.
    Assert.assertEquals(false, orientSSM.delete(id2));

  }

  /**
   * Tests OrientDB on whether it can set after deleting the key.
   * @throws InjectionException
   */
  @Test
  public void orientSetDeleteTest() throws InjectionException {
    //open orient db on a new path
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(OrientDbPath.class, "plocal:/tmp/SSM-orientdbTest");
    injector.bindVolatileParameter(OrientDbDropDb.class, true);
    injector.bindVolatileParameter(OrientDbSize.class, 100);
    SSMImplOrientDb orientSSM = injector.getInstance(SSMImplOrientDb.class);

    Identifier id1 = new StringIdentifierFactory().getNewInstance("Operator1");
    Integer value1 = 1;

    //Check if set works after deletion.
    orientSSM.delete(id1);
    orientSSM.set(id1, value1);
    Assert.assertEquals(value1, orientSSM.get(id1));
  }
}