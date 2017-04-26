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

import edu.snu.mist.common.parameters.MQTTBrokerURI;
import edu.snu.mist.common.parameters.MQTTTopic;
import edu.snu.mist.common.shared.MQTTSharedResource;
import org.apache.reef.tang.annotations.Parameter;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * This class publishes MQTT messages to MQTT broker.
 */
public final class MqttSink implements Sink<MqttMessage> {
  /**
   * MQTT publisher map that has broker URI as a key and MQTT client as a value.
   */
  private final ConcurrentMap<String, MqttClient> mqttPublisherMap;

  /**
   * MQTT publisher client.
   */
  private final MqttClient mqttClient;

  /**
   * MQTT topic.
   */
  private final String topic;

  @Inject
  public MqttSink(
      @Parameter(MQTTBrokerURI.class) final String brokerURI,
      @Parameter(MQTTTopic.class) final String topic,
      final MQTTSharedResource sharedResource) throws IOException, MqttException {
    this.topic = topic;
    this.mqttPublisherMap = sharedResource.getMqttPublisherMap();
    if (mqttPublisherMap.get(brokerURI) == null) {
      synchronized (this) {
        // Still MQTT Client is not created.
        if (mqttPublisherMap.get(brokerURI) == null) {
          // TODO:[MIST-495] Improve Mqtt sink parallelism
          // As we create a single client for each broker,
          // the client could be a bottleneck when there are lots of topics.
          final MqttClient client = new MqttClient(brokerURI, MQTTSharedResource.MQTT_PUBLISHER_ID + brokerURI);
          client.connect();
          mqttPublisherMap.putIfAbsent(brokerURI, client);
        }
      }
    }
    this.mqttClient = mqttPublisherMap.get(brokerURI);
  }

  @Override
  public void close() throws Exception {
    // TODO:[MIST-494] Safely close MQTT publisher client.
  }

  @Override
  public void handle(final MqttMessage input) {
    try {
      mqttClient.publish(topic, input);
    } catch (MqttException e) {
      e.printStackTrace();
    }
  }
}
