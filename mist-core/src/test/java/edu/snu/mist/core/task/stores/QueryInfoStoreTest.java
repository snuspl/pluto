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
import edu.snu.mist.core.master.ApplicationCodeManager;
import edu.snu.mist.core.parameters.DriverHostname;
import edu.snu.mist.core.parameters.MasterToDriverPort;
import edu.snu.mist.core.parameters.SharedStorePath;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.core.task.groupaware.ApplicationInfo;
import edu.snu.mist.core.task.groupaware.ApplicationMap;
import edu.snu.mist.core.task.utils.MockMasterToDriverMessage;
import edu.snu.mist.core.utils.TestParameters;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.AvroVertex;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.MasterToDriverMessage;
import org.apache.avro.ipc.Server;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryInfoStoreTest {

  private static final String DRIVER_HOSTNAME = "127.0.0.1";

  private static final int DRIVER_PORT = 33333;

  private Server mockMasterToDriverServer;

  @Before
  public void startUp() {
    mockMasterToDriverServer = AvroUtils.createAvroServer(MasterToDriverMessage.class,
        new MockMasterToDriverMessage(), new InetSocketAddress(DRIVER_HOSTNAME, DRIVER_PORT));
  }

  @After
  public void tearOff() {
    mockMasterToDriverServer.close();
  }

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

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DriverHostname.class, DRIVER_HOSTNAME);
    jcb.bindNamedParameter(MasterToDriverPort.class, String.valueOf(DRIVER_PORT));
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final QueryInfoStore store = injector.getInstance(QueryInfoStore.class);
    final ApplicationCodeManager applicationCodeManager = injector.getInstance(ApplicationCodeManager.class);
    final ApplicationMap applicationMap = injector.getInstance(ApplicationMap.class);

    final String queryId1 = "testQuery1";
    final String queryId2 = "testQuery2";
    final String tmpFolderPath = injector.getNamedInstance(SharedStorePath.class);
    final File folder = new File(tmpFolderPath);

    // Store jar files

    final List<String> paths = applicationCodeManager.registerNewAppCode(jarFiles).getJarPaths();
    for (int i = 0; i < jarFiles.size(); i++) {
      final ByteBuffer buf = ByteBuffer.allocateDirect(jarFiles.get(i).capacity());
      final String path = paths.get(i);
      final FileInputStream fis = new FileInputStream(path);
      final FileChannel channel = fis.getChannel();
      channel.read(buf);
      Assert.assertEquals(jarFiles.get(i), buf);
    }

    final ApplicationInfo applicationInfo = mock(ApplicationInfo.class);
    when(applicationInfo.getApplicationId()).thenReturn(TestParameters.SUPER_GROUP_ID);
    when(applicationInfo.getJarFilePath()).thenReturn(paths);
    applicationMap.putIfAbsent(TestParameters.SUPER_GROUP_ID, applicationInfo);

    // Generate logical plan
    final Tuple<List<AvroVertex>, List<Edge>> serializedDag = query.getAvroOperatorDag();
    final AvroDag.Builder avroDagBuilder = AvroDag.newBuilder();
    final AvroDag avroDag1 = avroDagBuilder
        .setAppId(TestParameters.SUPER_GROUP_ID)
        .setQueryId(TestParameters.QUERY_ID)
        .setJarPaths(paths)
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .build();
    final AvroDag avroDag2 = avroDagBuilder
        .setAppId(TestParameters.SUPER_GROUP_ID)
        .setQueryId(TestParameters.QUERY_ID)
        .setJarPaths(paths)
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
