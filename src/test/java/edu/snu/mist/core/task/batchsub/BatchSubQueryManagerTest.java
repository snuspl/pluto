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
package edu.snu.mist.core.task.batchsub;

import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.api.batchsub.BatchSubmissionConfiguration;
import edu.snu.mist.api.datastreams.ContinuousStream;
import edu.snu.mist.api.datastreams.configurations.MQTTSourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.rpc.RPCServerPort;
import edu.snu.mist.core.driver.parameters.ExecutionModelOption;
import edu.snu.mist.core.parameters.PlanStorePath;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.globalsched.GroupAwareGlobalSchedQueryManagerImpl;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.core.task.threadbased.ThreadBasedQueryManagerImpl;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
import edu.snu.mist.formats.avro.AvroVertexChain;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.JarUploadResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.mockito.Matchers;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * TODO[DELETE] this code is for test.
 * Test batch submission in the query managers of option 1, 2, and 3.
 */
public final class BatchSubQueryManagerTest {
  private QueryManager manager;
  private Tuple<List<String>, AvroOperatorChainDag> tuple;
  private List<String> groupIdList;
  private Injector injector;
  private AvroConfigurationSerializer avroConfigurationSerializer;
  private static final String QUERY_ID_PREFIX = "TestQueryId";
  private static final String ORIGINAL_GROUP_ID = "OriginalGroupId";
  private static final String ORIGINAL_PUB_TOPIC = "OriginalPubTopic";
  private static final String ORIGINAL_SUB_TOPIC1 = "OriginalSubTopic1";
  private static final String ORIGINAL_SUB_TOPIC2 = "OriginalSubTopic2";
  private static final String BROKER_URI = "tcp://localhost:12345";
  private static final int NUM_QUERIES = 101;
  private static final MISTFunction<MqttMessage, Tuple<MqttMessage, Long>> EXTRACT_FUNC
      = (msg) -> new Tuple<>(msg, 10L);
  private static final MISTFunction<MqttMessage, MqttMessage> MAP_FUNC =
      (msg) -> new MqttMessage("TestData".getBytes());
  private static final MISTBiFunction<String, String, String> PUB_TOPIC_FUNCTION = (groupId, queryId) ->
      new StringBuilder("/group")
          .append(groupId)
          .append("/device")
          .append(queryId)
          .append("/pub")
          .toString();
  private static final MISTBiFunction<String, String, Set<String>> SUB_TOPIC_FUNCTION = (groupId, queryId) -> {
        final Set<String> topicList = new HashSet<>();
        topicList.add(new StringBuilder("/group")
            .append(groupId)
            .append("/device")
            .append(queryId + "_1")
            .append("/sub")
            .toString());
        topicList.add(new StringBuilder("/group")
            .append(groupId)
            .append("/device")
            .append(queryId + "_2")
            .append("/sub")
            .toString());
        return topicList;
      };

  /**
   * Build a simple query and make the AvroOperatorChainDag.
   * The simple query will consists of:
   * mqttSrc1 -> union -> map -> mqttSink
   * mqttSrc2
   * This query will be duplicated and the group id, topic configuration of source and sink will be overwritten.
   */
 //@Before
  public void setUp() throws Exception {
    // Make batch submission configuration
    // Because the size is 101, two threads will deal with this submission
    groupIdList = new LinkedList<>();
    for (int i = 0; i < NUM_QUERIES; i++) {
      groupIdList.add(String.valueOf(i));
    }
    final BatchSubmissionConfiguration batchSubConfig = new BatchSubmissionConfiguration(
        SUB_TOPIC_FUNCTION, PUB_TOPIC_FUNCTION, groupIdList);

    // Create MQTT query having original configuration
    final SourceConfiguration sourceConfiguration1 = MQTTSourceConfiguration.newBuilder()
        .setBrokerURI(BROKER_URI)
        .setTopic(ORIGINAL_SUB_TOPIC1)
        .setTimestampExtractionFunction(EXTRACT_FUNC)
        .build();
    final SourceConfiguration sourceConfiguration2 = MQTTSourceConfiguration.newBuilder()
        .setBrokerURI(BROKER_URI)
        .setTopic(ORIGINAL_SUB_TOPIC2)
        .setTimestampExtractionFunction(EXTRACT_FUNC)
        .build();
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder(ORIGINAL_GROUP_ID);
    final ContinuousStream<MqttMessage> mqttStream1 = queryBuilder.mqttStream(sourceConfiguration1);
    final ContinuousStream<MqttMessage> mqttStream2 = queryBuilder.mqttStream(sourceConfiguration2);
    mqttStream1.union(mqttStream2)
        .map(MAP_FUNC)
        .mqttOutput(BROKER_URI, ORIGINAL_PUB_TOPIC);
    final MISTQuery query = queryBuilder.build();

    // Make fake jar upload result
    final List<String> paths = new LinkedList<>();
    final JarUploadResult jarUploadResult = JarUploadResult.newBuilder()
        .setIsSuccess(true)
        .setMsg("Success")
        .setPaths(paths)
        .build();

    // Create AvroOperatorChainDag
    final Tuple<List<AvroVertexChain>, List<Edge>> serializedDag = query.getAvroOperatorChainDag();
    final AvroOperatorChainDag operatorChainDag = AvroOperatorChainDag.newBuilder()
        .setJarFilePaths(jarUploadResult.getPaths())
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .setGroupId(query.getGroupId())
        .setPubTopicGenerateFunc(
            SerializeUtils.serializeToString(batchSubConfig.getPubTopicGenerateFunc()))
        .setSubTopicGenerateFunc(
            SerializeUtils.serializeToString(batchSubConfig.getSubTopicGenerateFunc()))
        .setGroupIdList(batchSubConfig.getGroupIdList())
        .build();

    // Create query id list
    final List<String> queryIdList = new LinkedList<>();
    for (int i = 0; i < NUM_QUERIES; i++) {
      queryIdList.add(QUERY_ID_PREFIX + i);
    }
    tuple = new Tuple<>(queryIdList, operatorChainDag);
  }

  //@After
  public void tearDown() throws Exception {
    // Close the query manager
    manager.close();
    // Delete plan directory and plans
    deletePlans();
  }

  /**
   * Test option 1 query manager.
   */
  //@Test(timeout = 5000)
  public void testSubmitComplexQueryInOption1() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(RPCServerPort.class, "20332");
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "4");
    jcb.bindNamedParameter(ExecutionModelOption.class, "1");
    jcb.bindImplementation(QueryManager.class, GroupAwareQueryManagerImpl.class);
    injector = Tang.Factory.getTang().newInjector(jcb.build());
    testBatchSubmitQueryHelper();
  }

  /**
   * Test option 2 query manager.
   */
  //@Test(timeout = 5000)
  public void testSubmitComplexQueryInOption2() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(RPCServerPort.class, "20333");
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "4");
    jcb.bindNamedParameter(ExecutionModelOption.class, "2");
    jcb.bindImplementation(QueryManager.class, GroupAwareGlobalSchedQueryManagerImpl.class);
    injector = Tang.Factory.getTang().newInjector(jcb.build());
    final MistPubSubEventHandler pubSubEventHandler = injector.getInstance(MistPubSubEventHandler.class);
    new TestGroupEventHandler(pubSubEventHandler);
    testBatchSubmitQueryHelper();
  }

  /**
   * Test option 3 query manager.
   */
  //@Test(timeout = 5000)
  public void testSubmitComplexQueryInOption3() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(RPCServerPort.class, "20334");
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "4");
    jcb.bindNamedParameter(ExecutionModelOption.class, "3");
    jcb.bindImplementation(QueryManager.class, ThreadBasedQueryManagerImpl.class);
    injector = Tang.Factory.getTang().newInjector(jcb.build());
    testBatchSubmitQueryHelper();
  }

  /**
   * Test whether the query manager re-configure the MQTT source and sink properly.
   */
  private void testBatchSubmitQueryHelper() throws Exception {
    avroConfigurationSerializer = injector.getInstance(AvroConfigurationSerializer.class);

    // Create a TestDagGenerator. It checks whether the operator chain dag is modified properly.
    final DagGenerator dagGenerator = new TestDagGenerator();

    // Build QueryManager and create queries in batch manner
    manager = queryManagerBuild(tuple, dagGenerator);
    manager.createBatch(tuple);
  }

  /**
   * A builder for QueryManager.
   */
  private QueryManager queryManagerBuild(final Tuple<List<String>, AvroOperatorChainDag> tp,
                                         final DagGenerator dagGenerator) throws Exception {
    // Create mock PlanStore. It returns true and the above logical plan
    final QueryInfoStore planStore = mock(QueryInfoStore.class);
    when(planStore.saveAvroOpChainDag(Matchers.any())).thenReturn(true);
    when(planStore.load(Matchers.any())).thenReturn(tp.getValue());

    // Create QueryManager
    injector.bindVolatileInstance(DagGenerator.class, dagGenerator);
    injector.bindVolatileInstance(QueryInfoStore.class, planStore);

    // Submit the fake logical plan
    // The operators in the physical plan are executed
    final QueryManager queryManager = injector.getInstance(QueryManager.class);

    return queryManager;
  }

  /**
   * Deletes logical plans and a plan folder.
   */
  private void deletePlans() throws Exception {
    // Delete plan directory and plans
    final String planStorePath = injector.getNamedInstance(PlanStorePath.class);
    final File planFolder = new File(planStorePath);
    if (planFolder.exists()) {
      final File[] destroy = planFolder.listFiles();
      for (final File des : destroy) {
        des.delete();
      }
      planFolder.delete();
    }
  }

  /**
   * A simple group event handler for test.
   */
  private final class TestGroupEventHandler implements EventHandler<GroupEvent> {

    private TestGroupEventHandler(final MistPubSubEventHandler pubSubEventHandler) {
      pubSubEventHandler.getPubSubEventHandler().subscribe(GroupEvent.class, this);
    }

    @Override
    public void onNext(final GroupEvent groupEvent) {
      // Do nothing
    }
  }

  /**
   * A dag generator for testing the modification of operator chain.
   */
  private final class TestDagGenerator implements DagGenerator {

    private TestDagGenerator() {
      // do nothing
    }

    /*
    @Override
    public DAG<ExecutionVertex, MISTEdge> generate(final Tuple<String, AvroOperatorChainDag> queryIdAndAvroLogicalDag)
        throws IOException, InjectionException, ClassNotFoundException {
      final String queryId = queryIdAndAvroLogicalDag.getKey();
      final AvroOperatorChainDag opChainDag = queryIdAndAvroLogicalDag.getValue();
      final String actualGroupId = opChainDag.getGroupId();
      final Set<String> expectedSubTopicSet =
          SUB_TOPIC_FUNCTION.apply(actualGroupId, queryId);

      // Test whether the group id is overwritten well
      Assert.assertFalse(ORIGINAL_GROUP_ID.equals(actualGroupId));
      // Test whether the MQTT configuration is overwritten well
      for (final AvroVertexChain avroVertexChain : opChainDag.getAvroVertices()) {
        switch (avroVertexChain.getAvroVertexChainType()) {
          case SOURCE: {
            final Vertex vertex = avroVertexChain.getVertexChain().get(0);
            final Configuration modifiedConf = avroConfigurationSerializer.fromString(vertex.getConfiguration());

            // Restore the configuration and see whether it is overwritten well
            final Injector newInjector = Tang.Factory.getTang().newInjector(modifiedConf);
            final String mqttBrokerURI = newInjector.getNamedInstance(MQTTBrokerURI.class);
            final String mqttActualSubTopic = newInjector.getNamedInstance(MQTTTopic.class);
            final String serializedTimestampFunc = newInjector.getNamedInstance(SerializedTimestampExtractUdf.class);
            final MISTFunction<MqttMessage, Tuple<MqttMessage, Long>> timestampFunc =
                SerializeUtils.deserializeFromString(serializedTimestampFunc);

            // The broker URI should not be overwritten
            Assert.assertEquals(BROKER_URI, mqttBrokerURI);

            // The topic should be overwritten
            boolean matched = false;
            final Iterator<String> itr = expectedSubTopicSet.iterator();
            while(itr.hasNext()) {
              final String expectedSubTopic = itr.next();
              if (expectedSubTopic.equals(mqttActualSubTopic)) {
                itr.remove();
                matched = true;
                break;
              }
            }
            Assert.assertTrue(matched);

            // The timestamp extract function should not be modified
            final MqttMessage tmpMsg = new MqttMessage();
            final Tuple<MqttMessage, Long> expectedTuple = EXTRACT_FUNC.apply(tmpMsg);
            final Tuple<MqttMessage, Long> actualTuple = timestampFunc.apply(tmpMsg);
            Assert.assertEquals(expectedTuple.getKey(), actualTuple.getKey());
            Assert.assertEquals(expectedTuple.getValue(), actualTuple.getValue());
            break;
          }
          case OPERATOR_CHAIN: {
            // Do nothing
            break;
          }
          case SINK: {
            final Vertex vertex = avroVertexChain.getVertexChain().get(0);
            final Configuration modifiedConf = avroConfigurationSerializer.fromString(vertex.getConfiguration());

            // Restore the configuration and see whether it is overwritten well
            final Injector newInjector = Tang.Factory.getTang().newInjector(modifiedConf);
            final String mqttBrokerURI = newInjector.getNamedInstance(MQTTBrokerURI.class);
            final String mqttPubTopic = newInjector.getNamedInstance(MQTTTopic.class);

            // The broker URI should not be overwritten
            Assert.assertEquals(BROKER_URI, mqttBrokerURI);
            // The topic should be overwritten
            Assert.assertEquals(
                PUB_TOPIC_FUNCTION.apply(actualGroupId, queryId), mqttPubTopic);
            break;

          }
          default: {
            throw new IllegalArgumentException("MISTTest: Invalid vertex type");
          }
        }
      }
      
      return new AdjacentListConcurrentMapDAG<>();
    }
    */

    @Override
    public DAG<ExecutionVertex, MISTEdge> generate(final DAG<ConfigVertex, MISTEdge> configDag,
                                                   final List<String> jarFilePaths)
        throws IOException, ClassNotFoundException, InjectionException {
      return null;
    }
  }
}