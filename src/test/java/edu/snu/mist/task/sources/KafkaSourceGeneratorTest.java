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

package edu.snu.mist.task.sources;

import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.sources.parameters.KafkaTopicName;
import edu.snu.mist.task.sources.parameters.SourceId;
import edu.snu.mist.task.sources.parameters.ZkSourceAddress;
import edu.snu.mist.task.sources.parameters.ZkSourcePort;
import junit.framework.Assert;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class KafkaSourceGeneratorTest {
  /**
   * Zookeeper port for the Kafka consumer to connect to. 2181 is the default Zookeeper port.
   */
  private final int zkPort = 2181;

  /**
   * Zookeeper address for the Kafka consumer to connect to.
   */
  private final String zkAddress = "localhost";

  /**
   * Kafka port for the Kafka consumer to connect to. 9092 is the default Kafka Server port.
   */
  private final int kafkaPort = 9092;

  /**
   * Kafka address for the Kafka consumer to connect to.
   */
  private final String kafkaAddress = "localhost";

  /**
   * Test whether TextKafkaStreamGenerator fetches input stream
   * from the Kafka server and generates data correctly.
   * @throws Exception
   */
  @Test
  public void testKafkaSourceGenerator() throws Exception {
    final List<String> inputStream = Arrays.asList(
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "In in leo nec erat fringilla mattis eu non massa.",
        "Cras quis diam suscipit, commodo enim id, pulvinar nunc.");
    final ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
    final List<String> result = new LinkedList<>();

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(QueryId.class, "testQuery");
    jcb.bindNamedParameter(SourceId.class, "testSource");
    jcb.bindNamedParameter(ZkSourceAddress.class, zkAddress);
    jcb.bindNamedParameter(KafkaTopicName.class, "testTopic");
    jcb.bindNamedParameter(ZkSourcePort.class,  Integer.toString(zkPort));
    jcb.bindImplementation(SourceGenerator.class, TextKafkaStreamGenerator.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());

    //Try is used to auto-close the sourceGenerator.
    try (final SourceGenerator<String> sourceGenerator = injector.getInstance(SourceGenerator.class)) {
      final CountDownLatch latch = new CountDownLatch(inputStream.size());

      serverExecutor.execute(new Runnable() {
        public void run() {
          sourceGenerator.setOutputEmitter((data) -> {
            result.add(data);
            //The consumer counts down the latch when the data is received by the outputEmitter.
            latch.countDown();
          });
          //The sourceGenerator and the consumer starts.
          sourceGenerator.start();
        }
      });

      //Producer property setup. Producer connects to KafkaServer, not the zookeeper server.
      final Properties props = new Properties();
      props.put("metadata.broker.list", String.format("%s:%s", kafkaAddress, kafkaPort));
      props.put("serializer.class", "kafka.serializer.StringEncoder");

      final ProducerConfig producerConfig = new ProducerConfig(props);
      final Producer<String, String> producer = new Producer<>(producerConfig);

      final KeyedMessage<String, String> message1 = new KeyedMessage<>("testTopic", inputStream.get(0));
      final KeyedMessage<String, String> message2 = new KeyedMessage<>("testTopic", inputStream.get(1));
      final KeyedMessage<String, String> message3 = new KeyedMessage<>("testTopic", inputStream.get(2));

      //Producer sends the messages to the KafkaServer.
      producer.send(message1);
      producer.send(message2);
      producer.send(message3);

      //This thread waits for the consumer to receive the inputs.
      latch.await();

      producer.close();
      Assert.assertEquals("Result was : " + result, inputStream, result);
    }
  }
}
