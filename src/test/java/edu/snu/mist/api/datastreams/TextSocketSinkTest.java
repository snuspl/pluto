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
package edu.snu.mist.api.datastreams;

import edu.snu.mist.api.APITestParameters;
import edu.snu.mist.api.AvroVertexSerializable;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * The test class for TextSocketSinkConfiguration.
 */
public class TextSocketSinkTest {
  /**
   * Test for SinkImpl.
   */
  @Test
  public void testSinkImpl() {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final BaseSourceStream<String> sourceStream =
        queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final MISTQuery query = queryBuilder.build();

    final Sink sink = sourceStream.textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);
    Assert.assertSame(sink.getSinkConfiguration(), APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);

    // Check src -> sink
    final DAG<AvroVertexSerializable, Direction> dag = query.getDAG();
    final Map<AvroVertexSerializable, Direction> neighbors = dag.getEdges(sourceStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(Direction.LEFT, neighbors.get(sink));
  }
}
