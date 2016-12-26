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
package edu.snu.mist.api;

import edu.snu.mist.api.datastreams.KafkaSourceStream;
import edu.snu.mist.api.datastreams.TextSocketSourceStream;
import edu.snu.mist.api.datastreams.configurations.KafkaSourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.PeriodicWatermarkConfiguration;
import edu.snu.mist.api.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.WatermarkConfiguration;
import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * This class builds MIST query.
 */
public final class MISTQueryBuilder {

  /**
   * DAG of the query.
   */
  private final DAG<AvroVertexSerializable, Direction> dag;

  /**
   * Period of default watermark represented in milliseconds.
   */
  private static final int DEFAULT_WATERMARK_PERIOD = 100;

  /**
   * Expected delay of default watermark represented in milliseconds.
   */
  private static final int DEFAULT_EXPECTED_DELAY = 0;

  /**
   * The default watermark configuration.
   */
  private <T> PeriodicWatermarkConfiguration<T> getDefaultWatermarkConf() {
    return PeriodicWatermarkConfiguration.<T>newBuilder()
        .setWatermarkPeriod(DEFAULT_WATERMARK_PERIOD)
        .setExpectedDelay(DEFAULT_EXPECTED_DELAY)
        .build();
  }

  public MISTQueryBuilder() {
    this.dag = new AdjacentListDAG<>();
  }

  /**
   * Get socket text stream.
   * @param sourceConf socket text source
   * @return source stream
   */
  public TextSocketSourceStream socketTextStream(final TextSocketSourceConfiguration sourceConf) {
    return socketTextStream(sourceConf, getDefaultWatermarkConf());
  }

  /**
   * Get socket text stream.
   * @param sourceConf socket text source
   * @param watermarkConf watermark configuration
   * @return source stream
   */
  public TextSocketSourceStream socketTextStream(final TextSocketSourceConfiguration sourceConf,
                                                 final WatermarkConfiguration<String> watermarkConf) {
    final TextSocketSourceStream sourceStream = new TextSocketSourceStream(sourceConf, dag, watermarkConf);
    dag.addVertex(sourceStream);
    return sourceStream;
  }

  /**
   * Get a kafka source stream.
   * @param kafkaConf kafka source configuration
   * @param <K> the type of kafka record's key
   * @param <V> the type of kafka record's value
   * @return source stream
   */
  public <K, V> KafkaSourceStream<K, V> kafkaStream(final KafkaSourceConfiguration<K, V> kafkaConf) {
    return kafkaStream(kafkaConf, getDefaultWatermarkConf());
  }

  /**
   * Get a kafka source stream.
   * @param kafkaConf kafka source configuration
   * @param watermarkConf watermark configuration
   * @param <K> the type of kafka record's key
   * @param <V> the type of kafka record's value
   * @return source stream
   */
  public <K, V> KafkaSourceStream<K, V> kafkaStream(final KafkaSourceConfiguration<K, V> kafkaConf,
                                                    final WatermarkConfiguration<ConsumerRecord<K, V>> watermarkConf) {
    final KafkaSourceStream<K, V> kafkaStream = new KafkaSourceStream<>(kafkaConf, dag, watermarkConf);
    dag.addVertex(kafkaStream);
    return kafkaStream;
  }

  /**
   * Build the query.
   * @return the query
   */
  public MISTQuery build() {
    return new MISTQueryImpl(dag);
  }
}
