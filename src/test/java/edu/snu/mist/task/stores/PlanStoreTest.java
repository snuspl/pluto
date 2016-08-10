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

package edu.snu.mist.task.stores;

import edu.snu.mist.api.APITestParameters;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.formats.avro.*;
import edu.snu.mist.task.parameters.PlanStorePath;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class PlanStoreTest {
  /**
   * Tests whether the PlanStore correctly saves, deletes and loads logical plan.
   * @throws InjectionException
   * @throws IOException
   */
  @Test
  public void testDiskPlanStore() throws InjectionException, IOException {
    // Generate a query
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .flatMap(s -> Arrays.asList(s.split(" ")))
        .filter(s -> s.startsWith("A"))
        .map(s -> new Tuple2<>(s, 1))
        .reduceByKey(0, String.class, (Integer x, Integer y) -> x + y)
        .textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);
    final MISTQuery query = queryBuilder.build();
    // Generate logical plan
    final Tuple<List<Vertex>, List<Edge>> serializedDag = query.getSerializedDAG();
    final LogicalPlan.Builder logicalPlanBuilder = LogicalPlan.newBuilder();
    final LogicalPlan logicalPlan = logicalPlanBuilder
        .setIsJarSerialized(false)
        .setJar(ByteBuffer.wrap(new byte[1]))
        .setVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector();
    final PlanStore planStore = injector.getInstance(PlanStore.class);
    final String queryId = "planStoreTestQuery";
    final String planStorePath = injector.getNamedInstance(PlanStorePath.class);
    final File planFolder = new File(planStorePath);

    planStore.save(new Tuple<>(queryId, logicalPlan));
    Assert.assertTrue(new File(planStorePath, queryId + ".plan").exists());

    final LogicalPlan loadedPlan = planStore.load(queryId);
    Assert.assertEquals(logicalPlan.getIsJarSerialized(), loadedPlan.getIsJarSerialized());
    Assert.assertEquals(logicalPlan.getEdges(), loadedPlan.getEdges());
    Assert.assertEquals(logicalPlan.getJar(), loadedPlan.getJar());
    Assert.assertEquals(logicalPlan.getSchema(), loadedPlan.getSchema());
    testVerticesEqual(logicalPlan.getVertices(), loadedPlan.getVertices());
    planStore.delete(queryId);
    Assert.assertFalse(new File(planStorePath, queryId + ".plan").exists());

    planFolder.delete();
  }

  /**
   * Tests that two lists of vertices are equal.
   * @param vertecies the first list of vertices
   * @param loadedVertecies the second list of vertices
   */
  private void testVerticesEqual(final List<Vertex> vertecies, final List<Vertex> loadedVertecies) {
    for(int i=0; i<vertecies.size(); i++) {
      final Vertex vertex = vertecies.get(i);
      final Vertex loadedVertex = loadedVertecies.get(i);
      Assert.assertEquals(vertex.getSchema(), loadedVertex.getSchema());
      Assert.assertEquals(vertex.getVertexType(), loadedVertex.getVertexType());

      if (vertex.getAttributes() instanceof SourceInfo) {
        final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
        final SourceInfo loadedSourceInfo = (SourceInfo) loadedVertex.getAttributes();
        final Map<String, Object> stringConf = new HashMap<>();
        for (final Map.Entry<CharSequence, Object> entry : loadedSourceInfo.getWatermarkConfiguration().entrySet()) {
          stringConf.put(entry.getKey().toString(), entry.getValue());
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
