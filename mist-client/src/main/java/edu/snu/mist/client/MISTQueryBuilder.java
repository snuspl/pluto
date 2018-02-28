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
package edu.snu.mist.client;

import edu.snu.mist.client.datastreams.ContinuousStream;
import edu.snu.mist.client.datastreams.ContinuousStreamImpl;
import edu.snu.mist.client.datastreams.MISTStream;
import edu.snu.mist.client.datastreams.configurations.PeriodicWatermarkConfiguration;
import edu.snu.mist.client.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.WatermarkConfiguration;
import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * This class builds MIST query.
 */
public final class MISTQueryBuilder {

  /**
   * DAG of the query.
   */
  private final DAG<MISTStream, MISTEdge> dag;

  /**
   * Period of default watermark represented in milliseconds.
   */
  private static final int DEFAULT_WATERMARK_PERIOD = 1000;

  /**
   * Expected delay of default watermark represented in milliseconds.
   */
  private static final int DEFAULT_EXPECTED_DELAY = 0;

  /**
   * The super group id of the query.
   */
  private final String superGroupId;

  /**
   * The default watermark configuration.
   */
  private WatermarkConfiguration getDefaultWatermarkConf() {
    return PeriodicWatermarkConfiguration.newBuilder()
        .setWatermarkPeriod(DEFAULT_WATERMARK_PERIOD)
        .setExpectedDelay(DEFAULT_EXPECTED_DELAY)
        .build();
  }

  public MISTQueryBuilder(final String superGroupId) {
    this.dag = new AdjacentListDAG<>();
    this.superGroupId = superGroupId;
  }

  /**
   * Build a new continuous stream connected with the source.
   * @param sourceConf source configuration
   * @param watermarkConf watermark configuration
   * @param <T> stream type
   * @return a new continuous stream connected with the source
   */
  private <T> ContinuousStream<T> buildStream(final Configuration sourceConf,
                                              final Configuration watermarkConf) {
    final ContinuousStream<T> sourceStream =
        new ContinuousStreamImpl<>(dag, Configurations.merge(sourceConf, watermarkConf));
    dag.addVertex(sourceStream);
    return sourceStream;
  }

  /**
   * Create a continuous stream that receives data from the socket server.
   * @param srcConf source configuration
   * @return a new continuous stream
   */
  public ContinuousStream<String> socketTextStream(final SourceConfiguration srcConf) {
    return socketTextStream(srcConf, getDefaultWatermarkConf());
  }

  /**
   * Create a continuous stream that receives data from the socket server.
   * @param srcConf source configuration
   * @param watermarkConf a watermark configuration
   * @return a new continuous stream
   */
  public ContinuousStream<String> socketTextStream(final SourceConfiguration srcConf,
                                                   final WatermarkConfiguration watermarkConf) {
    assert srcConf.getType() == SourceConfiguration.SourceType.SOCKET;
    return buildStream(srcConf.getConfiguration(), watermarkConf.getConfiguration());
  }

  /**
   * Create a continuous stream that receives data from the kafka producer.
   * @param srcConf kafka configuration
   * @return a new continuous stream
   */
  public <K, V> ContinuousStream<ConsumerRecord<K, V>> kafkaStream(final SourceConfiguration srcConf) {
    return kafkaStream(srcConf, getDefaultWatermarkConf());
  }

  /**
   * Create a continuous stream that receives data from the kafka producer.
   * @param srcConf kafka configuration
   * @param watermarkConf a watermark configuration
   * @return a new continuous stream
   */
  public <K, V> ContinuousStream<ConsumerRecord<K, V>> kafkaStream(final SourceConfiguration srcConf,
                                                                   final WatermarkConfiguration watermarkConf) {
    assert srcConf.getType() == SourceConfiguration.SourceType.KAFKA;
    return buildStream(srcConf.getConfiguration(), watermarkConf.getConfiguration());
  }

  /**
   * Create a continuous stream that subscribes data from MQTT broker.
   * @param srcConf mqtt configuration
   * @return a new continuous stream
   */
  public ContinuousStream<MqttMessage> mqttStream(final SourceConfiguration srcConf) {
    return mqttStream(srcConf, getDefaultWatermarkConf());
  }

  /**
   * Create a continuous stream that subscribes data from MQTT broker.
   * @param srcConf mqtt configuration
   * @param watermarkConf a watermark configuration
   * @return a new continuous stream
   */
  public ContinuousStream<MqttMessage> mqttStream(final SourceConfiguration srcConf,
                                                  final WatermarkConfiguration watermarkConf) {
    assert srcConf.getType() == SourceConfiguration.SourceType.MQTT;
    return buildStream(srcConf.getConfiguration(), watermarkConf.getConfiguration());
  }

  /**
   * Build the query.
   * @return the query
   */
  public MISTQuery build() {
    return new MISTQueryImpl(dag, superGroupId);
  }
}
