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

import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public final class InvertedVertexIndexTest {

  /**
   * Test HashMapBasedInvertedVertexIndex.
   */
  @Test
  public void testHashMapBasedInvertedVertexIndex() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final InvertedVertexIndex invertedVertexIndex = injector.getInstance(HashMapInvertedVertexIndex.class);

    final PhysicalVertex v1 = mock(PhysicalVertex.class);
    final DAG<PhysicalVertex, Direction> dag1 = mock(DAG.class);

    // Create
    Assert.assertTrue(invertedVertexIndex.create(v1, dag1));
    Assert.assertEquals(dag1, invertedVertexIndex.read(v1));

    // Update
    final DAG<PhysicalVertex, Direction> dag2 = mock(DAG.class);
    invertedVertexIndex.update(v1, dag2);

    Assert.assertEquals(dag2, invertedVertexIndex.read(v1));

    // Delete
    Assert.assertEquals(dag2, invertedVertexIndex.delete(v1));
  }
}
