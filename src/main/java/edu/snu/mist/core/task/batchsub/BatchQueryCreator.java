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

import edu.snu.mist.api.datastreams.configurations.*;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.MQTTBrokerURI;
import edu.snu.mist.common.parameters.SerializedTimestampExtractUdf;
import edu.snu.mist.core.task.ClassLoaderProvider;
import edu.snu.mist.core.task.QueryManager;
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
import java.util.List;

/**
 * TODO[DELETE] this code is for test.
 * A batch query creator class for supporting test.
 */
public final class BatchQueryCreator {

  /**
   * A configuration serializer.
   */
  private final AvroConfigurationSerializer avroConfigurationSerializer;

  /**
   * A classloader provider.
   */
  private final ClassLoaderProvider classLoaderProvider;

  @Inject
  private BatchQueryCreator(final AvroConfigurationSerializer avroConfigurationSerializer,
                            final ClassLoaderProvider classLoaderProvider) {
    this.avroConfigurationSerializer = avroConfigurationSerializer;
    this.classLoaderProvider = classLoaderProvider;
  }

  /**
   * Duplicate the submitted queries.
   *
   * @param tuple a pair of query id list and the operator chain dag
   * @param manager a query manager
   * @return submission result
   */
  public void duplicate(final Tuple<List<String>, AvroOperatorChainDag> tuple,
                        final QueryManager manager) throws Exception {
    final List<String> queryIdList = tuple.getKey();
    final AvroOperatorChainDag operatorChainDag = tuple.getValue();

    // Get classloader
    final URL[] urls = SerializeUtils.getJarFileURLs(operatorChainDag.getJarFilePaths());
    final ClassLoader classLoader = classLoaderProvider.newInstance(urls);

    // Load the batch submission configuration
    final MISTFunction<String, String> pubTopicFunc = SerializeUtils.deserializeFromString(
        operatorChainDag.getPubTopicGenerateFunc(), classLoader);
    final MISTFunction<String, String> subTopicFunc = SerializeUtils.deserializeFromString(
        operatorChainDag.getSubTopicGenerateFunc(), classLoader);
    final List<Integer> queryGroupList = operatorChainDag.getQueryGroupList();
    final int startQueryNum = operatorChainDag.getStartQueryNum();

    // Calculate the starting point
    int group = -1;
    int sum = 0;
    final Iterator<Integer> itr = queryGroupList.iterator();
    while (itr.hasNext() && sum <= startQueryNum) {
      final int groupQuery = itr.next();
      sum += groupQuery;
      group++;
    }
    // The remaining query to start in the starting group
    int remain = sum - startQueryNum + 1;
    String newGroupId = String.valueOf(group);
    String pubTopic = pubTopicFunc.apply(newGroupId);
    String subTopic = subTopicFunc.apply(newGroupId);

    for (int i = 0; i < queryIdList.size(); i++) {
      // Overwrite the group id
      operatorChainDag.setGroupId(newGroupId);
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
            final MQTTSourceConfiguration.MQTTSourceConfigurationBuilder builder =
                MQTTSourceConfiguration.newBuilder();

            try {
              final MISTFunction extractFuncClass = injector.getInstance(MISTFunction.class);
              //
              throw new RuntimeException(
                  "Class-based timestamp extract function config in batch submission is not allowed at now.");
            } catch (final InjectionException e) {
              // Function class is not defined
            }

            try {
              final String serializedFunction = injector.getNamedInstance(SerializedTimestampExtractUdf.class);
              // Timestamp function was set
              final MISTFunction<MqttMessage, Tuple<MqttMessage, Long>> extractFunc =
                  SerializeUtils.deserializeFromString(
                      injector.getNamedInstance(SerializedTimestampExtractUdf.class), classLoader);
              builder
                  .setTimestampExtractionFunction(extractFunc);
            } catch (final InjectionException e) {
              // Timestamp function was not set
            }

            final Configuration modifiedConf;
            modifiedConf = builder
                .setBrokerURI(mqttBrokerURI)
                .setTopic(subTopic)
                .build().getConfiguration();
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
      final QueryControlResult result = manager.create(newTuple);
      if (!result.getIsSuccess()) {
        throw new RuntimeException(i + "'th duplicated query creation failed.");
      }

      remain--;
      if (remain <= 0) {
        if (itr.hasNext()) {
          remain = itr.next();
          group++;
          newGroupId = String.valueOf(group);
          pubTopic = pubTopicFunc.apply(newGroupId);
          subTopic = subTopicFunc.apply(newGroupId);
        } else {
          throw new RuntimeException("The query group list does not have enough queries");
        }
      }
    }
  }
}