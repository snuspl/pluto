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
package edu.snu.mist.core.task.threadbased;

import edu.snu.mist.api.datastreams.configurations.MQTTSourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.MqttSinkConfiguration;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.parameters.MQTTBrokerURI;
import edu.snu.mist.common.parameters.SerializedTimestampExtractUdf;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
import edu.snu.mist.formats.avro.AvroVertexChain;
import edu.snu.mist.formats.avro.QueryControlResult;
import edu.snu.mist.formats.avro.Vertex;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.implementation.java.ClassHierarchyImpl;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.inject.Inject;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This QueryManager is aware of the group and manages queries per group.
 */
@SuppressWarnings("unchecked")
public final class ThreadBasedQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(ThreadBasedQueryManagerImpl.class.getName());

  /**
   * Scheduler for periodic watermark emission.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * A plan store.
   */
  private final QueryInfoStore planStore;

  /**
   * A execution and logical dag generator.
   */
  private final DagGenerator dagGenerator;

  /**
   * Map that has the Operator chain as a key and the thread as a value.
   */
  private final Map<OperatorChain, Thread> threads;

  /**
   * A configuration serializer.
   */
  private final AvroConfigurationSerializer avroConfigurationSerializer;

  /**
   * A classloader provider.
   */
  private final ClassLoaderProvider classLoaderProvider;

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private ThreadBasedQueryManagerImpl(final DagGenerator dagGenerator,
                                      final ScheduledExecutorServiceWrapper schedulerWrapper,
                                      final QueryInfoStore planStore,
                                      final AvroConfigurationSerializer avroConfigurationSerializer,
                                      final ClassLoaderProvider classLoaderProvider) {
    this.dagGenerator = dagGenerator;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.threads = new ConcurrentHashMap<>();
    this.avroConfigurationSerializer = avroConfigurationSerializer;
    this.classLoaderProvider = classLoaderProvider;
  }

  /**
   * It converts the avro operator chain dag (query) to the execution dag,
   * and executes the sources in order to receives data streams.
   * Before the queries are executed, it stores the avro operator chain dag into disk.
   * We can regenerate the queries from the stored avro operator chain dag.
   * @param tuple a pair of the query id and the avro operator chain dag
   * @return submission result
   */
  private void createSingleQuery(final Tuple<String, AvroOperatorChainDag> tuple) throws Exception {
    // 1) Saves the avro operator chain dag to the PlanStore and
    // converts the avro operator chain dag to the logical and execution dag
    planStore.saveAvroOpChainDag(tuple);
    final DAG<ExecutionVertex, MISTEdge> executionDag = dagGenerator.generate(tuple);
    // Execute the execution dag
    start(executionDag);
  }

  /**
   * Create a submitted query.
   * @param tuple the query id and the operator chain dag
   * @return submission result
   */
  @Override
  public QueryControlResult create(final Tuple<String, AvroOperatorChainDag> tuple) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(tuple.getKey());
    try {
      // Create the submitted query
      createSingleQuery(tuple);
      
      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(tuple.getKey()));
      return queryControlResult;
    } catch (final Exception e) {
      // [MIST-345] We need to release all of the information that is required for the query when it fails.
      LOG.log(Level.SEVERE, "An exception occurred while starting {0} query: {1}",
          new Object[] {tuple.getKey(), e.getMessage()});
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(e.getMessage());
      return queryControlResult;
    }
  }

  /**
   * Start submitted queries in batch manner.
   * The operator chain dag will be duplicated for test.
   * @param tuple a pair of query id list and the operator chain dag
   * @return submission result
   */
  @Override
  public QueryControlResult batchCreate(final Tuple<List<String>, AvroOperatorChainDag> tuple) {
    final List<String> queryIdList = tuple.getKey();
    final AvroOperatorChainDag operatorChainDag = tuple.getValue();
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryIdList.get(0));
    try {
      // Get classloader
      final URL[] urls = SerializeUtils.getJarFileURLs(operatorChainDag.getJarFilePaths());
      final ClassLoader classLoader = classLoaderProvider.newInstance(urls);

      // Load the batch submission configuration
      final MISTFunction<String, String> pubTopicFunc = SerializeUtils.deserializeFromString(
          new String(operatorChainDag.getPubTopicGenerateFunc().array()), classLoader);
      final MISTFunction<String, String> subTopicFunc = SerializeUtils.deserializeFromString(
          new String(operatorChainDag.getSubTopicGenerateFunc().array()), classLoader);
      final List<Integer> queryGroupList = operatorChainDag.getQueryGroupList();
      final int startQueryNum = operatorChainDag.getStartQueryNum();

      // Calculate the starting point
      int group = -1;
      int sum = 0;
      final Iterator<Integer> itr = queryGroupList.iterator();
      while(itr.hasNext() && sum <= startQueryNum) {
        final int groupQuery = itr.next();
        sum += groupQuery;
        group++;
      }
      int remain = sum - startQueryNum;
      String newGroupId = String.valueOf(group);
      String pubTopic = pubTopicFunc.apply(newGroupId);
      String subTopic = subTopicFunc.apply(newGroupId);

      for (int i = 0; i < queryIdList.size(); i++) {
        // Insert the topic information to a copied AvroOperatorChainDag
        for (final AvroVertexChain avroVertexChain : operatorChainDag.getAvroVertices()) {
          switch (avroVertexChain.getAvroVertexChainType()) {
            case SOURCE: {
              // It have to be MQTT source at now
              final Vertex vertex = avroVertexChain.getVertexChain().get(0);
              final Configuration originConf = avroConfigurationSerializer.fromString(vertex.getConfiguration(),
                  new ClassHierarchyImpl(urls));

              // Restore the original configuration and inject the overriding topic
              final Injector injector = Tang.Factory.getTang().newInjector(originConf);
              final String mqttBrokerURI = injector.getNamedInstance(MQTTBrokerURI.class);
              final String serializedFunction = injector.getNamedInstance(SerializedTimestampExtractUdf.class);

              try {
                final MISTFunction extractFuncClass = injector.getInstance(MISTFunction.class);
                if (extractFuncClass != null) {
                  throw new RuntimeException("Class-based source config in batch submission is not allowed at now.");
                }
              } catch (final InjectionException e) {
                // Function class is not defined
              }

              final Configuration modifiedConf;
              if (serializedFunction == null) {
                modifiedConf = MQTTSourceConfiguration.newBuilder()
                    .setBrokerURI(mqttBrokerURI)
                    .setTopic(pubTopic)
                    .build().getConfiguration();
              } else {
                final MISTFunction<MqttMessage, Tuple<MqttMessage, Long>> extractFunc =
                    SerializeUtils.deserializeFromString(
                        injector.getNamedInstance(SerializedTimestampExtractUdf.class), classLoader);
                modifiedConf = MQTTSourceConfiguration.newBuilder()
                    .setBrokerURI(mqttBrokerURI)
                    .setTopic(subTopic)
                    .build().getConfiguration();
              }
              vertex.setConfiguration(avroConfigurationSerializer.toString(modifiedConf));
              break;
            }
            case OPERATOR_CHAIN: {
              // Do nothing
              break;
            }
            case SINK: {
              final Vertex vertex = avroVertexChain.getVertexChain().get(0);
              final Configuration originConf = avroConfigurationSerializer.fromString(vertex.getConfiguration(),
                  new ClassHierarchyImpl(urls));

              // Restore the original configuration and inject the overriding topic
              final Injector injector = Tang.Factory.getTang().newInjector(originConf);
              final String mqttBrokerURI = injector.getNamedInstance(MQTTBrokerURI.class);
              final Configuration modifiedConf = MqttSinkConfiguration.CONF
                  .set(MqttSinkConfiguration.MQTT_BROKER_URI, mqttBrokerURI)
                  .set(MqttSinkConfiguration.MQTT_TOPIC, pubTopic)
                  .build();
              vertex.setConfiguration(avroConfigurationSerializer.toString(modifiedConf));
              break;
            }
            default: {
              throw new IllegalArgumentException("MISTTask: Invalid vertex detected in AvroLogicalPlan!");
            }
          }
        }

        final Tuple<String, AvroOperatorChainDag> newTuple = new Tuple<>(queryIdList.get(i), operatorChainDag);
        createSingleQuery(newTuple);

        remain--;
        if (remain <= 0) {
          if (itr.hasNext()) {
            remain = itr.next();
            newGroupId = String.valueOf(group);
            pubTopic = pubTopicFunc.apply(newGroupId);
            subTopic = subTopicFunc.apply(newGroupId);
          } else {
            throw new RuntimeException("The query group list does not have enough queries");
          }
        }
      }

      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(tuple.getKey().get(0)));
      return queryControlResult;
    } catch (final Exception e) {
      e.printStackTrace();
      // [MIST-345] We need to release all of the information that is required for the query when it fails.
      LOG.log(Level.SEVERE, "An exception occurred while starting from {0} to {1} batch query: {1}",
          new Object[] {queryIdList.get(0), queryIdList.get(queryIdList.size() - 1), e.toString()});
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(e.getMessage());
      return queryControlResult;
    }
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   * @param physicalPlan physical plan of the query
   */
  private void start(final DAG<ExecutionVertex, MISTEdge> physicalPlan) {
    final List<PhysicalSource> sources = new LinkedList<>();
    final Iterator<ExecutionVertex> iterator = GraphUtils.topologicalSort(physicalPlan);
    while (iterator.hasNext()) {
      final ExecutionVertex executionVertex = iterator.next();
      switch (executionVertex.getType()) {
        case SOURCE: {
          final PhysicalSource source = (PhysicalSource)executionVertex;
          final Map<ExecutionVertex, MISTEdge> nextOps = physicalPlan.getEdges(source);
          // 3) Sets output emitters
          source.setOutputEmitter(new SourceOutputEmitter<>(nextOps));
          sources.add(source);
          break;
        }
        case OPERATOR_CHIAN: {
          // 2) Inserts the OperatorChain to OperatorChainManager.
          final OperatorChain operatorChain = (OperatorChain)executionVertex;
          final Map<ExecutionVertex, MISTEdge> edges =
              physicalPlan.getEdges(operatorChain);

          // Create a thread per operator chain
          final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
              while (!Thread.currentThread().isInterrupted()) {
                operatorChain.processNextEvent();
              }
            }
          });
          thread.start();
          threads.put(operatorChain, thread);
          // 3) Sets output emitters
          operatorChain.setOutputEmitter(new OperatorOutputEmitter(edges));
          break;
        }
        case SINK: {
          break;
        }
        default:
          throw new RuntimeException("Invalid vertex type: " + executionVertex.getType());
      }
    }

    // 4) starts to receive input data stream from the sources
    for (final PhysicalSource source : sources) {
      source.start();
    }
  }

  @Override
  public void close() throws Exception {
    scheduler.shutdown();
    for (final Thread thread : threads.values()) {
      thread.interrupt();
    }
  }

  /**
   * Deletes queries from MIST.
   */
  @Override
  public QueryControlResult delete(final String groupId, final String queryId) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryId);
    queryControlResult.setIsSuccess(true);
    queryControlResult.setMsg(ResultMessage.deleteSuccess(queryId));
    return queryControlResult;
  }
}
