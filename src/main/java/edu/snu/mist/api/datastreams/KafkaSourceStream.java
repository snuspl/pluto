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
package edu.snu.mist.api.datastreams;

import edu.snu.mist.api.AvroVertexSerializable;
import edu.snu.mist.api.datastreams.configurations.KafkaSourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.WatermarkConfiguration;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.SourceTypeEnum;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * This class represents a SourceStream getting the data from Kafka topic.
 * @param <K> the type of kafka record's key that kafka consumer would consume
 * @param <V> the type of kafka record's value that kafka consumer would consume
 */
public final class KafkaSourceStream<K, V> extends BaseSourceStream<ConsumerRecord<K, V>> {

  public KafkaSourceStream(final KafkaSourceConfiguration<K, V> kafkaSourceConfiguration,
                           final DAG<AvroVertexSerializable, Direction> dag,
                           final WatermarkConfiguration<ConsumerRecord<K, V>> watermarkConfiguration) {
    super(SourceTypeEnum.KAFKA_SOURCE, kafkaSourceConfiguration, dag, watermarkConfiguration);
  }
}