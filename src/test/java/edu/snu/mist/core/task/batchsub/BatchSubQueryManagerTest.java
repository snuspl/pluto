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
import edu.snu.mist.common.parameters.MQTTBrokerURI;
import edu.snu.mist.common.parameters.MQTTTopic;
import edu.snu.mist.common.parameters.SerializedTimestampExtractUdf;
import edu.snu.mist.common.rpc.RPCServerPort;
import edu.snu.mist.core.driver.parameters.ExecutionModelOption;
import edu.snu.mist.core.parameters.PlanStorePath;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.globalsched.GroupAwareGlobalSchedQueryManagerImpl;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.core.task.threadbased.ThreadBasedQueryManagerImpl;
import edu.snu.mist.core.task.threadpool.threadbased.ThreadPoolQueryManagerImpl;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.AvroVertex;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.JarUploadResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * TODO[DELETE] this code is for test.
 * Test batch submission in the query managers of option 1, 2, and 3.
 */
public final class BatchSubQueryManagerTest {
  private QueryManager manager;
  private Tuple<List<String>, AvroDag> tuple;
  private List<String> groupIdList;
  private Injector injector;
  private AvroConfigurationSerializer avroConfigurationSerializer;
  private AtomicBoolean duplicationSuccess;
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
  // This pub topic function is query-id unaware for testing
  private static final MISTBiFunction<String, String, String> PUB_TOPIC_FUNCTION = (groupId, queryId) ->
      new StringBuilder("/group")
          .append(groupId)
          .append("/device")
          .append("/pub")
          .toString();
  // This sub topic function is query-id unaware for testing
  private static final MISTBiFunction<String, String, Set<String>> SUB_TOPIC_FUNCTION = (groupId, queryId) -> {
        final Set<String> topicList = new HashSet<>();
        topicList.add(new StringBuilder("/group")
            .append(groupId)
            .append("/device")
            .append("_1")
            .append("/sub")
            .toString());
        topicList.add(new StringBuilder("/group")
            .append(groupId)
            .append("/device")
            .append("_2")
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
  @Before
  public void setUp() throws Exception {
    // Make batch submission configuration
    // Because the size is 101, two threads will deal with this submission
    groupIdList = new LinkedList<>();
    for (int i = 0; i < NUM_QUERIES; i++) {
      groupIdList.add(String.valueOf(i));
    }
    final BatchSubmissionConfiguration batchSubConfig = new BatchSubmissionConfiguration(
        SUB_TOPIC_FUNCTION, PUB_TOPIC_FUNCTION, groupIdList, 1);

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

    // Create AvroDag
    final Tuple<List<AvroVertex>, List<Edge>> serializedDag = query.getAvroOperatorDag();
    final AvroDag avroDag = AvroDag.newBuilder()
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
    tuple = new Tuple<>(queryIdList, avroDag);

    duplicationSuccess = new AtomicBoolean(true);
  }

  @After
  public void tearDown() throws Exception {
    // Close the query manager
    manager.close();
    // Delete plan directory and plans
    deletePlans();
  }

  /**
   * Test option 2 query manager.
   */
  @Test(timeout = 10000)
  public void testSubmitComplexQueryInMIST() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(RPCServerPort.class, "20333");
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "4");
    jcb.bindNamedParameter(ExecutionModelOption.class, "mist");
    jcb.bindImplementation(QueryManager.class, GroupAwareGlobalSchedQueryManagerImpl.class);
    injector = Tang.Factory.getTang().newInjector(jcb.build());
    final MistPubSubEventHandler pubSubEventHandler = injector.getInstance(MistPubSubEventHandler.class);
    new TestGroupEventHandler(pubSubEventHandler);
    testBatchSubmitQueryHelper();
  }

  /**
   * Test option 3 query manager.
   */
  @Test(timeout = 5000)
  public void testSubmitComplexQueryInThreadBased() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(RPCServerPort.class, "20334");
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "4");
    jcb.bindNamedParameter(ExecutionModelOption.class, "tpq");
    jcb.bindImplementation(QueryManager.class, ThreadBasedQueryManagerImpl.class);
    injector = Tang.Factory.getTang().newInjector(jcb.build());
    testBatchSubmitQueryHelper();
  }

  /**
   * Test thread pool.
   */
  @Test(timeout = 5000)
  public void testSubmitComplexQueryInThreadPool() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(RPCServerPort.class, "20335");
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "4");
    jcb.bindNamedParameter(ExecutionModelOption.class, "tp");
    jcb.bindImplementation(QueryManager.class, ThreadPoolQueryManagerImpl.class);
    injector = Tang.Factory.getTang().newInjector(jcb.build());
    testBatchSubmitQueryHelper();
  }

  /**
   * Test whether the query manager re-configure the MQTT source and sink properly.
   */
  private void testBatchSubmitQueryHelper() throws Exception {
    avroConfigurationSerializer = injector.getInstance(AvroConfigurationSerializer.class);

    // Create a TestDagGenerator. It checks whether the operator chain dag is modified properly.
    final ConfigDagGenerator dagGenerator = new TestDagGenerator();

    // Build QueryManager and create queries in batch manner
    manager = queryManagerBuild(tuple, dagGenerator);
    manager.createBatch(tuple);

    Assert.assertTrue(duplicationSuccess.get());
  }

  /**
   * A builder for QueryManager.
   */
  private QueryManager queryManagerBuild(final Tuple<List<String>, AvroDag> tp,
                                         final ConfigDagGenerator dagGenerator) throws Exception {
    // Create mock PlanStore. It returns true and the above logical plan
    final QueryInfoStore planStore = mock(QueryInfoStore.class);
    when(planStore.load(Matchers.any())).thenReturn(tp.getValue());

    // Create QueryManager
    injector.bindVolatileInstance(ConfigDagGenerator.class, dagGenerator);
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
  private final class TestDagGenerator implements ConfigDagGenerator {

    private TestDagGenerator() {
      // do nothing
    }

    @Override
    public DAG<ConfigVertex, MISTEdge> generate(final AvroDag avroDag) {
      try {
        final String actualGroupId = avroDag.getGroupId();
        final Set<String> expectedSubTopicSet =
            SUB_TOPIC_FUNCTION.apply(actualGroupId, "arbitrary");

        // Test whether the group id is overwritten well
        Assert.assertFalse(ORIGINAL_GROUP_ID.equals(actualGroupId));
        // Test whether the MQTT configuration is overwritten well
        for (final AvroVertex avroVertex : avroDag.getAvroVertices()) {
          switch (avroVertex.getAvroVertexType()) {
            case SOURCE: {
              final Configuration modifiedConf =
                  avroConfigurationSerializer.fromString(avroVertex.getConfiguration());

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
              while (itr.hasNext()) {
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
            case OPERATOR: {
              // Do nothing
              break;
            }
            case SINK: {
              final Configuration modifiedConf =
                  avroConfigurationSerializer.fromString(avroVertex.getConfiguration());

              // Restore the configuration and see whether it is overwritten well
              final Injector newInjector = Tang.Factory.getTang().newInjector(modifiedConf);
              final String mqttBrokerURI = newInjector.getNamedInstance(MQTTBrokerURI.class);
              final String mqttPubTopic = newInjector.getNamedInstance(MQTTTopic.class);

              // The broker URI should not be overwritten
              Assert.assertEquals(BROKER_URI, mqttBrokerURI);
              // The topic should be overwritten
              Assert.assertEquals(
                  PUB_TOPIC_FUNCTION.apply(actualGroupId, "arbitrary"), mqttPubTopic);
              break;
            }
            default: {
              throw new IllegalArgumentException("MISTTest: Invalid vertex type");
            }
          }
        }
      } catch (final Exception e) {
        e.printStackTrace();
        duplicationSuccess.compareAndSet(true, false);
      }

      return new AdjacentListConcurrentMapDAG<>();
    }
  }
}