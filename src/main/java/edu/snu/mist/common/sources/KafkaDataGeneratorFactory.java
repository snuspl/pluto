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
package edu.snu.mist.common.sources;

import edu.snu.mist.common.sources.parameters.NumKafkaThreads;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class creates new instances of KafkaDataGenerator.
 * It is designed to share a thread pool among kafka sources to reduce the number of I/O threads.
 */
public final class KafkaDataGeneratorFactory implements AutoCloseable {

  /**
   * The default timeout for consumer polling represented in milliseconds.
   */
  private static final int DEFAULT_POLL_TIMEOUT = 2000;

  /**
   * The executor service used to restrict the number of threads for kafka sources.
   */
  private ExecutorService executorService;

  /**
   * @param threads the number of I/O threads
   */
  @Inject
  private KafkaDataGeneratorFactory(@Parameter(NumKafkaThreads.class) final int threads) {
    this.executorService = Executors.newFixedThreadPool(threads);
  }

  /**
   * Create a new instance of kafka data generator.
   * @param topic the kafka topic to monitor
   * @param kafkaConsumerConf the KafkaConsumer configuration
   * @param <K> the type of kafka record's key
   * @param <V> the type of kafka record's value
   * @return a new data generator
   */
  public <K, V> KafkaDataGenerator<K, V> newDataGenerator(final String topic,
                                                          final Map<String, Object> kafkaConsumerConf)
      throws Exception {
    return new KafkaDataGenerator<>(topic, kafkaConsumerConf, executorService, DEFAULT_POLL_TIMEOUT);
  }

  @Override
  public void close() throws Exception {
    executorService.shutdown();
  }
}
