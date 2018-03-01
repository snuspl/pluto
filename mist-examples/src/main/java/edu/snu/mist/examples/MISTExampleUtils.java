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

package edu.snu.mist.examples;

import edu.snu.mist.client.APIQueryControlResult;
import edu.snu.mist.client.MISTDefaultExecutionEnvironmentImpl;
import edu.snu.mist.client.MISTExecutionEnvironment;
import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.client.datastreams.configurations.KafkaSourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.MQTTSourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.examples.parameters.DriverAddress;
import edu.snu.mist.formats.avro.JarUploadResult;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Common behavior and basic defaults for MIST examples.
 */
public final class MISTExampleUtils {
  /**
   * TCP endpoint for sink server.
   */
  public static final String SINK_HOSTNAME = "localhost";
  public static final int SINK_PORT = 20330;

  /**
   * Default kafka configuration values.
   */
  public static final String DEFAULT_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.IntegerDeserializer";
  public static final String DEFAULT_VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

  /**
   * Get a new sink server.
   */
  public static SinkServer getSinkServer() {
    return new SinkServer(SINK_PORT);
  }

  /**
   * Get socket configuration for local text source.
   */
  public static SourceConfiguration getLocalTextSocketSourceConf(final String socket) {
    final String[] sourceSocket = socket.split(":");
    final String sourceHostname = sourceSocket[0];
    final int sourcePort = Integer.parseInt(sourceSocket[1]);
    return TextSocketSourceConfiguration.newBuilder()
        .setHostAddress(sourceHostname)
        .setHostPort(sourcePort)
        .build();
  }

  /**
   * Get socket configuration for local kafka source.
   */
  public static SourceConfiguration getLocalKafkaSourceConf(final String topic,
                                                            final String socket) {
    return getLocalKafkaSourceConf(topic, socket, DEFAULT_KEY_DESERIALIZER, DEFAULT_VALUE_DESERIALIZER);
  }

  /**
   * Get socket configuration for local kafka source.
   */
  public static SourceConfiguration getLocalKafkaSourceConf(final String topic,
                                                            final String socket,
                                                            final String keyDeserializer,
                                                            final String valueDeserializer) {
    final HashMap<String, Object> kafkaConsumerConfig = new HashMap<>();
    kafkaConsumerConfig.put("bootstrap.servers", socket);
    kafkaConsumerConfig.put("group.id", "MistExample");
    kafkaConsumerConfig.put("key.deserializer", keyDeserializer);
    kafkaConsumerConfig.put("value.deserializer", valueDeserializer);
    return KafkaSourceConfiguration.newBuilder()
        .setTopic(topic)
        .setConsumerConfig(kafkaConsumerConfig)
        .build();
  }

  /**
   * Get socket configuration for MQTT source.
   */
  public static SourceConfiguration getMQTTSourceConf(final String topic,
                                                      final String brokerURI) {
    return MQTTSourceConfiguration.newBuilder()
        .setTopic(topic)
        .setBrokerURI(brokerURI)
        .build();
  }

  private static String getJarFilePath() throws URISyntaxException {
    final String path = MISTExampleUtils.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
    return path;
  }

  /**
   * Submit query to MIST driver.
   */
  public static APIQueryControlResult submit(final MISTQueryBuilder queryBuilder,
                                             final Configuration configuration)
      throws IOException, URISyntaxException, InjectionException {
    final String[] driverSocket =
        Tang.Factory.getTang().newInjector(configuration).getNamedInstance(DriverAddress.class).split(":");
    final String driverHostname = driverSocket[0];
    final int driverPort = Integer.parseInt(driverSocket[1]);

    try (final MISTExecutionEnvironment executionEnvironment =
        new MISTDefaultExecutionEnvironmentImpl(driverHostname, driverPort)) {

      // Upload jar
      final String jarFilePath = getJarFilePath();
      final List<String> jarFilePaths = Arrays.asList(jarFilePath);
      final JarUploadResult result = executionEnvironment.submitJar(jarFilePaths);
      queryBuilder.setApplicationId(result.getIdentifier());

      return executionEnvironment.submitQuery(queryBuilder.build());
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static CommandLine getDefaultCommandLine(final JavaConfigurationBuilder jcb) {
    return new CommandLine(jcb)
        .registerShortNameOfClass(DriverAddress.class);
  }

  /**
   * Must not be instantiated.
   */
  private MISTExampleUtils() {
  }
}