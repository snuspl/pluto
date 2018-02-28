/*
 * Copyright (C) 2018 Seoul National University
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

import edu.snu.mist.client.MISTQuery;
import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.core.parameters.TempFolderPath;
import edu.snu.mist.core.task.groupaware.GroupMap;
import edu.snu.mist.core.task.groupaware.MetaGroup;
import edu.snu.mist.core.utils.TestParameters;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.AvroVertex;
import edu.snu.mist.formats.avro.Edge;
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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryInfoStoreTest {
  /**
   * Tests whether the PlanStore correctly saves, deletes and loads the operator chain dag.
   * @throws InjectionException
   * @throws IOException
   */
  @Test(timeout = 1000)
  public void diskStoreTest() throws InjectionException, IOException {
    // Generate a query
    final MISTQueryBuilder queryBuilder =
        new MISTQueryBuilder();
    queryBuilder.setApplicationId(TestParameters.SUPER_GROUP_ID);
    queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .flatMap(s -> Arrays.asList(s.split(" ")))
        .filter(s -> s.startsWith("A"))
        .map(s -> new Tuple2<>(s, 1))
        .reduceByKey(0, String.class, (Integer x, Integer y) -> x + y)
        .textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);
    final MISTQuery query = queryBuilder.build();

    // Jar files
    final List<ByteBuffer> jarFiles = new LinkedList<>();
    final ByteBuffer byteBuffer1 = ByteBuffer.wrap(new byte[]{0, 1, 0, 1, 1, 1});
    jarFiles.add(byteBuffer1);

    final Injector injector = Tang.Factory.getTang().newInjector();
    final QueryInfoStore store = injector.getInstance(QueryInfoStore.class);
    final GroupMap groupMap = injector.getInstance(GroupMap.class);

    final String queryId1 = "testQuery1";
    final String queryId2 = "testQuery2";
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

    final MetaGroup metaGroup = mock(MetaGroup.class);
    when(metaGroup.getApplicationId()).thenReturn(TestParameters.SUPER_GROUP_ID);
    when(metaGroup.getJarFilePath()).thenReturn(paths);
    groupMap.putIfAbsent(TestParameters.SUPER_GROUP_ID, metaGroup);

    // Generate logical plan
    final Tuple<List<AvroVertex>, List<Edge>> serializedDag = query.getAvroOperatorDag();
    final AvroDag.Builder avroDagBuilder = AvroDag.newBuilder();
    final AvroDag avroDag1 = avroDagBuilder
        .setAppId(TestParameters.SUPER_GROUP_ID)
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .build();
    final AvroDag avroDag2 = avroDagBuilder
        .setAppId(TestParameters.SUPER_GROUP_ID)
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .build();

    // Store the chained dag
    store.saveAvroDag(new Tuple<>(queryId1, avroDag1));
    store.saveAvroDag(new Tuple<>(queryId2, avroDag2));
    while (!(store.isStored(queryId1) && store.isStored(queryId2))) {
      // Wait until the plan is stored
    }
    Assert.assertTrue(new File(tmpFolderPath, queryId1 + ".plan").exists());
    Assert.assertTrue(new File(tmpFolderPath, queryId2 + ".plan").exists());

    // Test stored file
    final AvroDag loadedDag1 = store.load(queryId1);
    Assert.assertEquals(avroDag1.getEdges(), loadedDag1.getEdges());
    Assert.assertEquals(avroDag1.getSchema(), loadedDag1.getSchema());
    testVerticesEqual(avroDag1.getAvroVertices(), loadedDag1.getAvroVertices());

    final AvroDag loadedDag2 = store.load(queryId2);
    Assert.assertEquals(avroDag2.getEdges(), loadedDag2.getEdges());
    Assert.assertEquals(avroDag2.getSchema(), loadedDag2.getSchema());
    testVerticesEqual(avroDag2.getAvroVertices(), loadedDag2.getAvroVertices());

    // Test deletion
    store.delete(queryId1);
    store.delete(queryId2);
    Assert.assertFalse(store.isStored(queryId1));
    Assert.assertFalse(new File(tmpFolderPath, queryId1 + ".plan").exists());
    Assert.assertFalse(store.isStored(queryId2));
    Assert.assertFalse(new File(tmpFolderPath, queryId2 + ".plan").exists());
    for (final String path : paths) {
      Assert.assertFalse(new File(path).exists());
    }
    folder.delete();
  }

  //@Test(timeout = 1000)
  public void hashCollisionTest() throws InjectionException, IOException {
    // Jar files
    final List<ByteBuffer> jarFiles1 = new LinkedList<>();
    final ByteBuffer byteBuffer1 = ByteBuffer.wrap(new byte[]{0, 1, 0, 1, 1, 1});
    final ByteBuffer byteBuffer2 = ByteBuffer.wrap(new byte[]{1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0});
    final ByteBuffer byteBuffer3 = ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1});
    jarFiles1.add(byteBuffer1);
    jarFiles1.add(byteBuffer2);
    jarFiles1.add(byteBuffer3);

    final List<ByteBuffer> jarFiles2 = new LinkedList<>();
    // byteBuffer 4 and 5 are the same as 1 and 2.
    final ByteBuffer byteBuffer4 = ByteBuffer.wrap(new byte[]{0, 1, 0, 1, 1, 1});
    final ByteBuffer byteBuffer5 = ByteBuffer.wrap(new byte[]{1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0});
    final ByteBuffer byteBuffer6 = ByteBuffer.wrap(new byte[]{1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 1, 0, 1, 1, 0});
    jarFiles2.add(byteBuffer4);
    jarFiles2.add(byteBuffer5);
    jarFiles2.add(byteBuffer6);

    final Injector injector = Tang.Factory.getTang().newInjector();
    final QueryInfoStore store = injector.getInstance(QueryInfoStore.class);
    final String tmpFolderPath = injector.getNamedInstance(TempFolderPath.class);
    final File folder = new File(tmpFolderPath);

    // Store jar files
    final List<String> paths1 = store.saveJar(jarFiles1);
    final List<String> paths2 = store.saveJar(jarFiles2);

    // Test if paths were redirected to the right files.
    Assert.assertEquals(paths2.get(0), paths1.get(0));
    Assert.assertEquals(paths2.get(1), paths1.get(1));
    Assert.assertNotEquals(paths2.get(2), paths1.get(2));

    folder.delete();
  }

  /**
   * Tests that two lists of vertices are equal.
   * @param vertices the first list of vertices
   * @param loadedVertices the second list of vertices
   */
  private void testVerticesEqual(final List<AvroVertex> vertices, final List<AvroVertex> loadedVertices) {
    for (int i = 0; i < vertices.size(); i++) {
      final AvroVertex avroVertex = vertices.get(i);
      final AvroVertex loadedVertex = loadedVertices.get(i);
      Assert.assertEquals(avroVertex.getConfiguration(), loadedVertex.getConfiguration());
      Assert.assertEquals(avroVertex.getSchema(), loadedVertex.getSchema());
    }
  }
}
