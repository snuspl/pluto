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
package edu.snu.mist.common;

import edu.snu.mist.common.parameters.VertexCodec;
import junit.framework.Assert;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.io.*;
import java.util.logging.Logger;

public final class DAGCodecTest {
  private static final Logger LOG = Logger.getLogger(DAGCodecTest.class.getName());
  /**
   * This test serializes the DAG and checks whether
   * the DAGCodec serializes/deserializes the DAG correctly or not.
   *
   * DAG:
   * A -> B -> C -> D
   *   -> E -> F-/
   * G -> H -/
   */
  @Test
  public void dagCodecTest() throws InjectionException, IOException {
    final DAG<String> dag = new AdjacentListDAG<>();
    dag.addVertex("A"); dag.addVertex("B");
    dag.addVertex("C"); dag.addVertex("D");
    dag.addVertex("E"); dag.addVertex("F");
    dag.addVertex("G"); dag.addVertex("H");

    dag.addEdge("A", "B");
    dag.addEdge("A", "E");
    dag.addEdge("B", "C");
    dag.addEdge("C", "D");
    dag.addEdge("E", "F");
    dag.addEdge("F", "D");
    dag.addEdge("G", "H");
    dag.addEdge("H", "F");

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(VertexCodec.class, new StringStreamingCodec());
    final StreamingCodec<DAG<String>> dagStreamingCodec = injector.getInstance(DAGCodec.class);
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (final DataOutputStream daos = new DataOutputStream(baos)) {
        dagStreamingCodec.encodeToStream(dag, daos);
      }
      final byte[] encodedDAG = baos.toByteArray();
      try (final ByteArrayInputStream bais = new ByteArrayInputStream(encodedDAG)) {
        try (final DataInputStream dais = new DataInputStream(bais)) {
          final DAG<String> decodedDAG = dagStreamingCodec.decodeFromStream(dais);
          LOG.info("expected: " + dag);
          LOG.info("actual: " + decodedDAG);
          Assert.assertEquals(dag, decodedDAG);
        }
      }
    }
  }

  /**
   * Test codec for String.
   */
  final class StringStreamingCodec implements StreamingCodec<String> {
    @Override
    public void encodeToStream(final String s, final DataOutputStream dataOutputStream) {
      SerializationUtils.serialize(s, dataOutputStream);
    }

    @Override
    public String decodeFromStream(final DataInputStream dataInputStream) {
      return (String)SerializationUtils.deserialize(dataInputStream);
    }

    @Override
    public String decode(final byte[] bytes) {
      return null;
    }

    @Override
    public byte[] encode(final String s) {
      return new byte[0];
    }
  }
}
