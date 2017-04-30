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
import edu.snu.mist.core.parameters.MaxInflightMqttEventNum;
import edu.snu.mist.core.parameters.MaxMqttSinkNumPerClient;
import org.apache.reef.tang.annotations.Parameter;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * This class publishes MQTT messages to MQTT broker.
 */
public final class MqttSink implements Sink<MqttMessage> {

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
      @Parameter(MaxMqttSinkNumPerClient.class) final int maxMqttSinkNumPerClient,
      @Parameter(MaxInflightMqttEventNum.class) final int maxInflightMqttEventNum,
      final MQTTSharedResource sharedResource) throws IOException, MqttException {

    final ConcurrentMap<String, List<MqttClient>> mqttPublisherMap = sharedResource.getMqttPublisherMap();
    final ConcurrentMap<MqttClient, Integer> clientSinkNumMap = sharedResource.getClientSinkNumMap();

    synchronized (mqttPublisherMap) {
      this.topic = topic;
      final List<MqttClient> mqttClientList = mqttPublisherMap.get(brokerURI);
      if (mqttClientList == null) {
        mqttPublisherMap.putIfAbsent(brokerURI, new ArrayList<>());
        final MqttClient client = new MqttClient(brokerURI, MQTTSharedResource.MQTT_PUBLISHER_ID_PREFIX
            + brokerURI + "_0");
        final MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setMaxInflight(maxInflightMqttEventNum);
        client.connect(connectOptions);
        mqttPublisherMap.get(brokerURI).add(client);
        clientSinkNumMap.put(client, 1);
        this.mqttClient = client;
      } else {
        // Pick the last client for the candidate.
        final MqttClient clientCandidate = mqttClientList.get(mqttClientList.size() - 1);
        final int sinkNum = clientSinkNumMap.get(clientCandidate);
        if (sinkNum < maxMqttSinkNumPerClient) {
          // It is okay to share already created mqtt client.
          clientSinkNumMap.replace(clientCandidate, sinkNum + 1);
          this.mqttClient = clientCandidate;
        } else {
          // We need to make a new mqtt client.
          final MqttClient newClientCandidate = new MqttClient(brokerURI, MQTTSharedResource.MQTT_PUBLISHER_ID_PREFIX +
              brokerURI + "_" + mqttClientList.size());
          final MqttConnectOptions connectOptions = new MqttConnectOptions();
          connectOptions.setMaxInflight(maxInflightMqttEventNum);
          newClientCandidate.connect(connectOptions);
          mqttClientList.add(newClientCandidate);
          clientSinkNumMap.put(newClientCandidate, 1);
          this.mqttClient = clientCandidate;
        }
      }
    }
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
