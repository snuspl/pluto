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

import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.common.shared.KafkaSharedResource;
import junit.framework.Assert;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.lang.Thread.sleep;

public final class KafkaSourceTest {
  /**
   * Kafka topic name for test.
   */
  private static final String KAFKA_TOPIC = "KafkaSourceTest";

  /**
   * Kafka port for the Kafka consumer to connect to.
   */
  private static final String KAFKA_PORT = "9123";

  /**
   * Kafka address for the Kafka consumer to connect to.
   */
  private static final String KAFKA_ADDRESS = "localhost";

  /**
   * Zookeeper port for the Kafka server to connect to.
   */
  private static final String ZK_PORT = "2123";

  /**
   * Zookeeper address for the Kafka server to connect to.
   */
  private static final String ZK_ADDRESS = "localhost";

  private KafkaSharedResource kafkaSharedResource;

  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    kafkaSharedResource = injector.getInstance(KafkaSharedResource.class);
  }

  @After
  public void tearDown() throws Exception {
    kafkaSharedResource.close();
  }

  /**
   * Test whether TextKafkaDataGenerator fetches input stream
   * from the Kafka server and generates data correctly.
   * @throws Exception
   */
  @Test(timeout = 15000L)
  public void testKafkaDataGenerator() throws Exception {
    final Map<String, String> inputStream = new HashMap<>();
    inputStream.put("0", "Lorem ipsum dolor sit amet, consectetur adipiscing elit.");
    inputStream.put("1", "In in leo nec erat fringilla mattis eu non massa.");
    inputStream.put("2", "Cras quis diam suscipit, commodo enim id, pulvinar nunc.");
    final CountDownLatch dataCountDownLatch = new CountDownLatch(inputStream.size());
    final Map<String, String> result = new HashMap<>();

    // create local kafka broker
    KafkaLocalBroker kafkaLocalBroker = new KafkaLocalBroker(KAFKA_PORT, KAFKA_ADDRESS, ZK_PORT, ZK_ADDRESS);
    kafkaLocalBroker.start();

    // define kafka consumer configuration
    final HashMap<String, Object> kafkaConsumerConf = new HashMap<>();
    kafkaConsumerConf.put("bootstrap.servers", kafkaLocalBroker.getLocalhostBroker());
    kafkaConsumerConf.put("group.id", "SourceTestGroup");
    kafkaConsumerConf.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaConsumerConf.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaConsumerConf.put("auto.offset.reset", "earliest");

    // define kafka producer configuration
    final HashMap<String, Object> kafkaProducerConf = new HashMap<>();
    kafkaProducerConf.put("bootstrap.servers", kafkaLocalBroker.getLocalhostBroker());
    kafkaProducerConf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProducerConf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    // create kafka source
    final KafkaDataGenerator<Integer, String> kafkaDataGenerator =
        new KafkaDataGenerator<>(KAFKA_TOPIC, kafkaConsumerConf, kafkaSharedResource);
    final SourceTestEventGenerator<String, String> eventGenerator =
        new SourceTestEventGenerator<>(result, dataCountDownLatch);
    kafkaDataGenerator.setEventGenerator(eventGenerator);
    kafkaDataGenerator.start();

    // create kafka producer
    final KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProducerConf);
    final ProducerRecord<String, String> record1 =
        new ProducerRecord<>(KAFKA_TOPIC, "0", inputStream.get("0"));
    final ProducerRecord<String, String> record2 =
        new ProducerRecord<>(KAFKA_TOPIC, "1", inputStream.get("1"));
    final ProducerRecord<String, String> record3 =
        new ProducerRecord<>(KAFKA_TOPIC, "2", inputStream.get("2"));
    producer.send(record1);
    producer.send(record2);
    producer.send(record3);
    producer.close();

    // wait for the consumer to receive the inputs.
    dataCountDownLatch.await();
    kafkaDataGenerator.close();
    // KafkaDataGenerator will wait until it's pollTimeout before it is closed.
    // Therefore, we need to wait a bit for KafkaDataGenerator.
    // TODO: [MIST-369] Removing sleep in the `KafkaSourceTest`
    sleep(2000);
    kafkaLocalBroker.stop();

    Assert.assertEquals(inputStream, result);
  }

  /**
   * Local Kafka broker for testing KafkaSourceTest.
   * TODO: [MIST-368] Change the logging way of kafka server and zookeeper in `KafkaLocalBroker`
   */
  private final class KafkaLocalBroker {
    /**
     * Logging directory paths' prefix.
     */
    private static final String DIR_PATH_PREFIX = "/tmp/mist/KafkaSourceTest-";
    /**
     * Logging directory name for kafka server.
     */
    private static final String KAFKA_DIR = "/kafka-logs/";
    /**
     * Snapping directory name for zookeeper server.
     */
    private static final String ZK_SNAP_DIR = "/zookeeper-snap/";
    /**
     * Logging directory name for zookeeper server.
     */
    private static final String ZK_LOG_DIR = "/zookeeper-log/";
    /**
     * Kafka port to connect to.
     */
    private final String kafkaPort;
    /**
     * Kafka address to connect to.
     */
    private final String kafkaAddress;
    /**
     * Zookeeper port to connect to.
     */
    private final String zkPort;
    /**
     * Zookeeper address to connect to.
     */
    private final String zkAddress;
    /**
     * The time that this broker is initiated.
     * It is used to create log directory name.
     */
    private final String initialTime;
    /**
     * Actual KafkaServer that this broker manages.
     */
    private KafkaServerStartable kafkaServer;
    /**
     * Zookeeper server factory that create a new zookeeper server.
     */
    private ServerCnxnFactory zkFactory;

    KafkaLocalBroker(final String kafkaPort,
                     final String kafkaAddress,
                     final String zkPort,
                     final String zkAddress) {
      this.kafkaPort = kafkaPort;
      this.kafkaAddress = kafkaAddress;
      this.zkPort = zkPort;
      this.zkAddress = zkAddress;
      this.initialTime = ((Long)System.currentTimeMillis()).toString();
    }

    /**
     * Start the broker.
     */
    void start() {
      // start zookeeper server
      final File snapDir = new File(DIR_PATH_PREFIX + initialTime + ZK_SNAP_DIR).getAbsoluteFile();
      final File logDir = new File(DIR_PATH_PREFIX + initialTime + ZK_LOG_DIR).getAbsoluteFile();
      try {
        final ZooKeeperServer server = new ZooKeeperServer(snapDir, logDir, 2000);
        zkFactory = new NIOServerCnxnFactory();
        zkFactory.configure(new InetSocketAddress(Integer.parseInt(zkPort)), 2000);
        zkFactory.startup(server);
      } catch (final Exception e) {
        e.printStackTrace();
      }

      // start kafka server
      final Properties kafkaProperties = new Properties();
      kafkaProperties.put("port", kafkaPort);
      kafkaProperties.put("broker.id", "0");
      kafkaProperties.put("log.dirs", DIR_PATH_PREFIX + initialTime + KAFKA_DIR);
      kafkaProperties.put("num.recovery.threads.per.data.dir", 1);
      kafkaProperties.put("zookeeper.connect", zkAddress + ":" + zkPort);
      kafkaProperties.put("controlled.shutdown.enable", true);
      kafkaProperties.put("host.name", "localhost");
      kafkaProperties.put("num.partitions", 1);
      kafkaServer = new KafkaServerStartable(KafkaConfig.fromProps(kafkaProperties));
      kafkaServer.startup();
    }

    /**
     * Stop the broker.
     */
    void stop(){
      kafkaServer.shutdown();
      zkFactory.closeAll();
      zkFactory.shutdown();
    }

    /**
     * @return the broker's connection.
     */
    String getLocalhostBroker() {
      return kafkaAddress + ":" + kafkaPort;
    }
  }

  /**
   * Event Generator for source test.
   * It stores the data which are sent from kafka data generator.
   * @param <K> the type of kafka record's key
   * @param <V> the type of kafka record's value
   */
  private final class SourceTestEventGenerator<K, V> implements EventGenerator<ConsumerRecord<K, V>> {
    private final Map<K, V> dataMap;
    private final CountDownLatch dataCountDownLatch;

    SourceTestEventGenerator(final Map<K, V> dataMap,
                             final CountDownLatch dataCountDownLatch) {
      this.dataMap = dataMap;
      this.dataCountDownLatch = dataCountDownLatch;
    }

    @Override
    public void emitData(final ConsumerRecord<K, V> input) {
      dataMap.put(input.key(), input.value());
      dataCountDownLatch.countDown();
    }

    @Override
    public void start() {
      // do nothing
    }

    @Override
    public void close() {
      // do nothing
    }

    @Override
    public void setOutputEmitter(final OutputEmitter emitter) {
      // do nothing
    }
  }
}
