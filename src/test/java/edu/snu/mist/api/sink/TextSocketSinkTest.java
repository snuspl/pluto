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
package edu.snu.mist.api.sink;

import edu.snu.mist.api.*;
import edu.snu.mist.api.sources.BaseSourceStream;
import edu.snu.mist.common.DAG;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * The test class for SinkConfiguration.
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
    Assert.assertEquals(sink.getSinkType(), StreamType.SinkType.TEXT_SOCKET_SINK);
    Assert.assertSame(sink.getSinkConfiguration(), APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);

    // Check src -> sink
    final DAG<AvroVertexSerializable, StreamType.Direction> dag = query.getDAG();
    final Map<AvroVertexSerializable, StreamType.Direction> neighbors = dag.getEdges(sourceStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(StreamType.Direction.LEFT, neighbors.get(sink));
  }
}
