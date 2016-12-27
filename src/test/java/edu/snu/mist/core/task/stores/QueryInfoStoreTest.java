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

package edu.snu.mist.core.task.stores;

import edu.snu.mist.api.APITestParameters;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.core.parameters.TempFolderPath;
import edu.snu.mist.formats.avro.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class QueryInfoStoreTest {
  /**
   * Tests whether the PlanStore correctly saves, deletes and loads logical plan.
   * @throws InjectionException
   * @throws IOException
   */
  @Test
  public void diskStoreTest() throws InjectionException, IOException {
    // Generate a query
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .flatMap(s -> Arrays.asList(s.split(" ")))
        .filter(s -> s.startsWith("A"))
        .map(s -> new Tuple2<>(s, 1))
        .reduceByKey(0, String.class, (Integer x, Integer y) -> x + y)
        .textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);
    final MISTQuery query = queryBuilder.build();

    // Jar files
    final List<ByteBuffer> jarFiles = new LinkedList<>();
    final ByteBuffer byteBuffer1 = ByteBuffer.wrap(new byte[]{0, 1, 0, 1, 1, 1});
    final ByteBuffer byteBuffer2 = ByteBuffer.wrap(new byte[]{1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0});
    jarFiles.add(byteBuffer1);
    jarFiles.add(byteBuffer2);

    final Injector injector = Tang.Factory.getTang().newInjector();
    final QueryInfoStore store = injector.getInstance(QueryInfoStore.class);
    final String queryId = "testQuery";
    final String tmpFolderPath = injector.getNamedInstance(TempFolderPath.class);
    final File folder = new File(tmpFolderPath);

    // Store jar files
    final List<String> paths = store.saveJar(jarFiles);
    for (int i = 0; i < jarFiles.size(); i++) {
      final ByteBuffer buf = ByteBuffer.allocateDirect(jarFiles.get(i).capacity());
      final String path = paths.get(i);
      final FileInputStream fis = new FileInputStream(path);
      final FileChannel channel = fis.getChannel();
      channel.read(buf);
      Assert.assertEquals(jarFiles.get(i), buf);
    }

    // Generate logical plan
    final Tuple<List<AvroVertexChain>, List<Edge>> serializedDag = query.getSerializedDAG();
    final LogicalPlan.Builder logicalPlanBuilder = LogicalPlan.newBuilder();
    final LogicalPlan logicalPlan = logicalPlanBuilder
        .setJarFilePaths(paths)
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .build();

    // Store the logical plan
    store.savePlan(new Tuple<>(queryId, logicalPlan));
    Assert.assertTrue(new File(tmpFolderPath, queryId + ".plan").exists());

    final LogicalPlan loadedPlan = store.load(queryId);
    Assert.assertEquals(logicalPlan.getEdges(), loadedPlan.getEdges());
    Assert.assertEquals(logicalPlan.getSchema(), loadedPlan.getSchema());
    testVerticesEqual(logicalPlan.getAvroVertices(), loadedPlan.getAvroVertices());
    store.delete(queryId);
    Assert.assertFalse(new File(tmpFolderPath, queryId + ".plan").exists());
    for (final String path : paths) {
      Assert.assertFalse(new File(path).exists());
    }
    folder.delete();
  }

  /**
   * Tests that two lists of vertices are equal.
   * @param vertices the first list of vertices
   * @param loadedVertices the second list of vertices
   */
  private void testVerticesEqual(final List<AvroVertexChain> vertices, final List<AvroVertexChain> loadedVertices) {
    for (int i = 0; i < vertices.size(); i++) {
      final AvroVertexChain avroVertexChain = vertices.get(i);
      final AvroVertexChain loadedVertexChain = loadedVertices.get(i);
      for (int j = 0; j < avroVertexChain.getVertexChain().size(); j++) {
        final Vertex vertex = avroVertexChain.getVertexChain().get(j);
        final Vertex loadedVertex = loadedVertexChain.getVertexChain().get(j);
        Assert.assertEquals(vertex.getSchema(), loadedVertex.getSchema());
        Assert.assertEquals(vertex.getVertexType(), loadedVertex.getVertexType());

        if (vertex.getAttributes() instanceof SourceInfo) {
          final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
          final SourceInfo loadedSourceInfo = (SourceInfo) loadedVertex.getAttributes();
          final Map<String, Object> stringConf = new HashMap<>();
          for (final Map.Entry<String, Object> entry : loadedSourceInfo.getWatermarkConfiguration().entrySet()) {
            stringConf.put(entry.getKey(), entry.getValue());
          }
          Assert.assertEquals(sourceInfo.getSchema(),
              loadedSourceInfo.getSchema());
          Assert.assertEquals(sourceInfo.getSourceType(),
              loadedSourceInfo.getSourceType());
          Assert.assertEquals(sourceInfo.getSourceConfiguration().toString(),
              loadedSourceInfo.getSourceConfiguration().toString());
          Assert.assertEquals(sourceInfo.getWatermarkType(),
              loadedSourceInfo.getWatermarkType());
          Assert.assertEquals(sourceInfo.getWatermarkConfiguration(), stringConf);
        } else {
          Assert.assertEquals(vertex.getAttributes().toString(), loadedVertex.getAttributes().toString());
        }
      }
    }
  }
}
