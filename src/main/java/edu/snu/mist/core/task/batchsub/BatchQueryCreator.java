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

import com.rits.cloning.Cloner;
import edu.snu.mist.api.datastreams.configurations.MQTTSourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.MqttSinkConfiguration;
import edu.snu.mist.api.datastreams.configurations.PunctuatedWatermarkConfiguration;
import edu.snu.mist.api.datastreams.configurations.WatermarkConfiguration;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.functions.WatermarkTimestampFunction;
import edu.snu.mist.common.parameters.MQTTBrokerURI;
import edu.snu.mist.common.parameters.MergeFakeParameter;
import edu.snu.mist.common.parameters.SerializedTimestampExtractUdf;
import edu.snu.mist.core.task.codeshare.ClassLoaderProvider;
import edu.snu.mist.core.task.QueryManager;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.AvroVertex;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.implementation.java.ClassHierarchyImpl;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.inject.Inject;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

  /**
   * A map which contains the class loaders for each super group.
   */
  private final ConcurrentMap<String, List<String>> superGroupJarPathMap;

  private final QueryInfoStore planStore;

  private final AtomicInteger duplicateNum;

  @Inject
  private BatchQueryCreator(final AvroConfigurationSerializer avroConfigurationSerializer,
                            final ClassLoaderProvider classLoaderProvider,
                            final QueryInfoStore planStore) {
    this.avroConfigurationSerializer = avroConfigurationSerializer;
    this.classLoaderProvider = classLoaderProvider;
    this.superGroupJarPathMap = new ConcurrentHashMap<>();
    this.planStore = planStore;
    this.duplicateNum = new AtomicInteger(0);
  }

  /**
   * Duplicate the submitted queries.
   *
   * @param tuple a pair of query id list and the operator chain dag
   * @param manager a query manager
   */
  public void duplicate(final Tuple<List<String>, AvroDag> tuple,
                        final QueryManager manager) throws Exception {
    
    final List<String> queryIdList = tuple.getKey();
    final AvroDag avroDag = tuple.getValue();
    final List<String> superGroupIdList = avroDag.getSuperGroupIdList();
    final List<String> subGroupIdList = avroDag.getSubGroupIdList();

    // Get classloader
    final URL[] jarUrls = SerializeUtils.getJarFileURLs(avroDag.getJarFilePaths());

    // Make new jars for new super groups
    for (final String superGroupId: superGroupIdList) {
      if (!superGroupJarPathMap.containsKey(superGroupId)) {
        final List<String> paths = new ArrayList<>();
        for (final String jarPath: avroDag.getJarFilePaths()) {
          final String newPath = String.format("/tmp/%d-duplicate.jar", duplicateNum.getAndIncrement());
          Files.copy(new File(jarPath).toPath(), new File(newPath).toPath());
          paths.add(newPath);
        }
        superGroupJarPathMap.putIfAbsent(superGroupId, paths);
        //System.err.println(String.format("Super Group Id = %s, Jar path: %s", superGroupId, paths));
      }
    }

    final ClassLoader tempClassLoader = classLoaderProvider.newInstance(jarUrls);

    // Load the batch submission configuration
    final MISTBiFunction<String, String, String> pubTopicFunc = SerializeUtils.deserializeFromString(
        avroDag.getPubTopicGenerateFunc(), tempClassLoader);
    final MISTBiFunction<String, String, Set<String>> subTopicFunc = SerializeUtils.deserializeFromString(
        avroDag.getSubTopicGenerateFunc(), tempClassLoader);


    // Parallelize the submission process
    final int batchThreads = superGroupIdList.size() / 100 + ((superGroupIdList.size() % 100 == 0) ? 0 : 1);
    final ExecutorService executorService = Executors.newFixedThreadPool(batchThreads);
    final List<Future> futures = new LinkedList<>();
    final boolean[] success = new boolean[batchThreads];

    final int mergeFactor = tuple.getValue().getMergeFactor();
    final List<String> fakeMergeIdList = new ArrayList<>(superGroupIdList.size());
    for (int i = 0; i < superGroupIdList.size(); i++) {
      fakeMergeIdList.add(String.valueOf(i % mergeFactor));
    }

    for (int i = 0; i < batchThreads; i++) {
      final int threadNum = i;
      final List<Configuration> originConfs = new ArrayList<>(avroDag.getAvroVertices().size());
      for (final AvroVertex avroVertex : avroDag.getAvroVertices()) {
        originConfs.add(avroConfigurationSerializer.fromString(
            avroVertex.getConfiguration(), new ClassHierarchyImpl(jarUrls)));
      }
      // Do a deep copy for avroDag
      final AvroDag avroDagClone = new Cloner().deepClone(avroDag);
      futures.add(executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            for (int j = threadNum * 100; j < (threadNum + 1) * 100 && j < queryIdList.size(); j++) {
              // Set the topic according to the query number
              final String superGroupId = superGroupIdList.get(j);
              final String subGroupId = subGroupIdList.get(j);
              final String queryId = queryIdList.get(j);
              final String pubTopic = pubTopicFunc.apply(superGroupId + "," + subGroupId, queryId);
              final Iterator<String> subTopicItr = subTopicFunc.apply(subGroupId, queryId).iterator();
              final String fakeMergeId = fakeMergeIdList.get(j);

              // Overwrite the group id
              avroDagClone.setSuperGroupId(superGroupId);
              avroDagClone.setSubGroupId(subGroupId);
              // Overwrite the jar path
              avroDagClone.setJarFilePaths(superGroupJarPathMap.get(superGroupId));
              // Insert the topic information to a copied AvroOperatorChainDag
              int vertexIndex = 0;
              for (final AvroVertex avroVertex : avroDagClone.getAvroVertices()) {
                switch (avroVertex.getAvroVertexType()) {
                  case SOURCE: {
                    // It have to be MQTT source at now
                    final Configuration originConf = avroConfigurationSerializer.fromString(
                        avroVertex.getConfiguration(), new ClassHierarchyImpl(jarUrls));

                    // Restore the original configuration and inject the overriding topic
                    final Injector injector = Tang.Factory.getTang().newInjector(originConf);
                    final String mqttBrokerURI = injector.getNamedInstance(MQTTBrokerURI.class);
                    final MQTTSourceConfiguration.MQTTSourceConfigurationBuilder builder =
                        MQTTSourceConfiguration.newBuilder();

                    // For test availability, the source won't send any watermark.
                    final Configuration funcConf = Tang.Factory.getTang().newConfigurationBuilder().build();
                    final WatermarkConfiguration defaultWatermarkConfig =
                        PunctuatedWatermarkConfiguration.<MqttMessage>newBuilder()
                            .setWatermarkPredicate(FalsePredicate.class, funcConf)
                            .setParsingWatermarkFunction(DefaultWatermarkTimestampExtractFunc.class, funcConf)
                            .build();
                    try {
                      final MISTFunction extractFuncClass = injector.getInstance(MISTFunction.class);
                      // Class-based timestamp extract function config in batch submission is not allowed at now
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
                              injector.getNamedInstance(SerializedTimestampExtractUdf.class), tempClassLoader);
                      builder
                          .setTimestampExtractionFunction(extractFunc);
                    } catch (final InjectionException e) {
                      // Timestamp function was not set
                    }

                    final Configuration modifiedDataGeneratorConf = builder
                        .setBrokerURI(mqttBrokerURI)
                        .setTopic(subTopicItr.next())
                        .build().getConfiguration();
                    final Configuration modifiedConf =
                        Configurations.merge(modifiedDataGeneratorConf, defaultWatermarkConfig.getConfiguration());

                    avroVertex.setConfiguration(avroConfigurationSerializer.toString(modifiedConf));
                    break;
                  }
                  case OPERATOR: {
                    // Configure fake parameter to
                    final Configuration originConf = originConfs.get(vertexIndex);
                    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
                    jcb.bindNamedParameter(MergeFakeParameter.class, fakeMergeId);
                    final Configuration mergedConf = Configurations.merge(originConf, jcb.build());
                    avroVertex.setConfiguration(avroConfigurationSerializer.toString(mergedConf));
                    break;
                  }
                  case SINK: {
                    final Configuration originConf = avroConfigurationSerializer.fromString(
                        avroVertex.getConfiguration(), new ClassHierarchyImpl(jarUrls));

                    // Restore the original configuration and inject the overriding topic
                    final Injector injector = Tang.Factory.getTang().newInjector(originConf);
                    final String mqttBrokerURI = injector.getNamedInstance(MQTTBrokerURI.class);
                    final Configuration modifiedConf = MqttSinkConfiguration.CONF
                        .set(MqttSinkConfiguration.MQTT_BROKER_URI, mqttBrokerURI)
                        .set(MqttSinkConfiguration.MQTT_TOPIC, pubTopic)
                        .build();
                    avroVertex.setConfiguration(avroConfigurationSerializer.toString(modifiedConf));
                    break;
                  }
                  default: {
                    throw new IllegalArgumentException("MISTTask: Invalid vertex detected in AvroLogicalPlan!");
                  }
                }
                vertexIndex += 1;
              }

              final Tuple<String, AvroDag> newTuple = new Tuple<>(queryIdList.get(j), avroDagClone);
              final QueryControlResult result = manager.create(newTuple);
              if (!result.getIsSuccess()) {
                throw new RuntimeException(j + "'th duplicated query creation failed: " + result.getMsg());
              }
            }
            // There was no exception during submission
            success[threadNum] = true;
          } catch (final Exception e) {
            e.printStackTrace();
            success[threadNum] = false;
            return;
          }
        }
      }));
    }

    // Wait to the end of submission and check whether every submission was successful or not
    boolean allSuccess = true;
    for (int i = 0; i < batchThreads; i++) {
      for (final Future future : futures) {
        while (!future.isDone()) {
          Thread.sleep(1000);
        }
        allSuccess = allSuccess && success[i];
      }
    }
    executorService.shutdown();

    if (!allSuccess) {
      throw new RuntimeException("Submission failed");
    }
  }

  /**
   * A predicate that always return false which will be used for evaluation.
   */
  private static final class FalsePredicate implements MISTPredicate<MqttMessage> {
    @Inject
    private FalsePredicate() {
      // do nothing
    }

    @Override
    public boolean test(final MqttMessage s) {
      return false;
    }
  }

  /**
   * A watermark timestamp function which will be used for evaluation.
   */
  private static final class DefaultWatermarkTimestampExtractFunc
      implements WatermarkTimestampFunction<MqttMessage> {
    @Inject
    private DefaultWatermarkTimestampExtractFunc() {
      // do nothing
    }

    @Override
    public Long apply(final MqttMessage s) {
      return 0L;
    }
  }
}