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
package edu.snu.mist.common.sinks;

import edu.snu.mist.common.shared.MQTTSharedResource;
import edu.snu.mist.common.utils.MqttUtils;
import io.moquette.server.Server;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.eclipse.paho.client.mqttv3.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public final class MqttSinkTest {

  /**
   * Shared resource for Mqtt publisher client.
   */
  private MQTTSharedResource mqttSharedResource;

  /**
   * Mqtt subscriber.
   */
  private MqttClient subscriber;

  /**
   * Mqtt Broker server.
   */
  private Server broker;

  @Before
  public void setUp() throws IOException, InjectionException, MqttException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    mqttSharedResource = injector.getInstance(MQTTSharedResource.class);
    broker = MqttUtils.createMqttBroker();
    subscriber = new MqttClient(MqttUtils.BROKER_URI, "mqttClient");
  }

  @After
  public void tearDown() throws Exception {
    mqttSharedResource.close();
    subscriber.disconnect();
    broker.stopServer();
  }

  /**
   * Test whether the created Mqtt sinks publish Mqtt messages to the broker.
   * It creates 4 sinks and send outputs to the broker.
   * @throws Exception
   */
  @Test(timeout = 4000L)
  public void testMqttSink() throws Exception {
    final int numSinks = 4;
    final List<String> outputStream = Arrays.asList(
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "In in leo nec erat fringilla mattis eu non massa.",
        "Cras quis diam suscipit, commodo enim id, pulvinar nunc.");
    final CountDownLatch countDownLatch = new CountDownLatch(numSinks * outputStream.size());
    final Map<String, List<String>> topicListMap = new ConcurrentHashMap<>();

    final String topic = "topic";
    subscriber.connect();
    subscriber.setCallback(new TestMqttSubscriber(topicListMap, countDownLatch));

    // Create sinks
    final List<Sink<MqttMessage>> sinks = new LinkedList<>();
    for (int i = 0; i < numSinks; i++) {
      final Sink<MqttMessage> publisher = new MqttSink(MqttUtils.BROKER_URI, topic+i, mqttSharedResource);
      topicListMap.put(topic+i, new LinkedList<>());
      sinks.add(publisher);
      subscriber.subscribe(topic+i);
    }

    outputStream.forEach((output) -> {
      for (final Sink<MqttMessage> sink : sinks) {
        sink.handle(new MqttMessage(output.getBytes()));
      }
    });

    // Wait until all data are sent to subscriber
    countDownLatch.await();
    for (List<String> received : topicListMap.values()) {
      Assert.assertEquals(outputStream, received);
    }

    // Closes
    for (final Sink<MqttMessage> sink : sinks) {
      sink.close();
    }
  }

  /**
   * Mqtt subscriber that is used in the sink test.
   */
  final class TestMqttSubscriber implements MqttCallback {
    private final Map<String, List<String>> result;
    private final CountDownLatch countDownLatch;

    public TestMqttSubscriber(final Map<String, List<String>> result,
                              final CountDownLatch countDownLatch) {
      this.result = result;
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void connectionLost(final Throwable cause) {
      // do nothing
    }

    @Override
    public void messageArrived(final String topic, final MqttMessage message) throws Exception {
      final List<String> l = result.get(topic);
      l.add(new String(message.getPayload()));
      countDownLatch.countDown();
    }

    @Override
    public void deliveryComplete(final IMqttDeliveryToken token) {
      // do nothing
    }
  }
}