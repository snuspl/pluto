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
package edu.snu.mist.core.replay;

import edu.snu.mist.core.utils.MqttUtils;
import io.moquette.server.Server;
import org.apache.reef.io.Tuple;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class EventReplayUtilsTest {

  @Test
  public void testSubscribeAndReplay() throws IOException, MqttException, InterruptedException {
    try {
      final int replayPortNum = 26523;
      final String localAddress = "127.0.0.1";
      final String topic1 = "source/user/gps";
      final String topic2 = "source/user/heartbeat";
      final String topic3 = "source/user/doorClosed";
      final String payload1 = "{\"word\": \"aaa\", \"start_timestamp\": 1}";
      final String payload2 = "{\"word\": \"bbb\", \"start_timestamp\": 2}";
      final String payload3 = "{\"word\": \"ccc\", \"start_timestamp\": 3}";
      final String payload4 = "{\"word\": \"ddd\", \"start_timestamp\": 4}";
      final String payload5 = "{\"word\": \"eee\", \"start_timestamp\": 5}";
      final String payload6 = "{\"word\": \"eee\", \"start_timestamp\": 6}";
      final MockReplayServer server = new MockReplayServer(replayPortNum);
      final Server mqttBroker = MqttUtils.createMqttBroker();
      final MqttClient pubClient = new MqttClient(MqttUtils.BROKER_URI, "testPubClient");

      final Thread serverThread = new Thread(new Runnable() {
        @Override
        public void run() {
          server.startServer();
        }
      });
      serverThread.start();

      // Wait for the socket to open.
      Thread.sleep(1000);
      while (server.isClosed()) {
        Thread.sleep(100);
      }
      Assert.assertTrue(!server.isClosed());

      // Have the local replay server subscribe to topics 1~3.
      EventReplayUtils.subscribe(localAddress, replayPortNum,
          MqttUtils.HOST, Integer.parseInt(MqttUtils.PORT), topic1);
      EventReplayUtils.subscribe(localAddress, replayPortNum,
          MqttUtils.HOST, Integer.parseInt(MqttUtils.PORT), topic2);
      EventReplayUtils.subscribe(localAddress, replayPortNum,
          MqttUtils.HOST, Integer.parseInt(MqttUtils.PORT), topic3);

      // Add mqtt events to the mock server.
      pubClient.connect();
      pubClient.publish(topic1, new MqttMessage(payload1.getBytes()));
      pubClient.publish(topic1, new MqttMessage(payload2.getBytes()));
      pubClient.publish(topic1, new MqttMessage(payload3.getBytes()));
      pubClient.publish(topic2, new MqttMessage(payload4.getBytes()));
      pubClient.publish(topic2, new MqttMessage(payload4.getBytes()));
      pubClient.publish(topic3, new MqttMessage(payload5.getBytes()));
      pubClient.publish(topic2, new MqttMessage(payload6.getBytes()));
      Thread.sleep(1000);

      // Fetch all events by using the replay method.
      final EventReplayResult result1 = EventReplayUtils.replay(localAddress, replayPortNum);
      final Map<String, Map<String, List<Tuple<Long, MqttMessage>>>> result1Map =
          result1.getBrokerAndTopicMqttMessageMap();
      Assert.assertEquals(1, result1Map.size());

      final Map<String, List<Tuple<Long, MqttMessage>>> result1BrokerMap = result1Map.get(MqttUtils.BROKER_URI);
      Assert.assertEquals(3, result1BrokerMap.size());
      final List<Tuple<Long, MqttMessage>> topic1Events = result1BrokerMap.get(topic1);
      Assert.assertEquals(3, topic1Events.size());
      final List<Tuple<Long, MqttMessage>> topic2Events = result1BrokerMap.get(topic2);
      Assert.assertEquals(3, topic2Events.size());
      final List<Tuple<Long, MqttMessage>> topic3Events = result1BrokerMap.get(topic3);
      Assert.assertEquals(1, topic3Events.size());

      // Fetch events from timestamp 3L ~ 5L using the replay method.
      final EventReplayResult result2 = EventReplayUtils.replay(localAddress, replayPortNum, 3L, 5L);
      final Map<String, Map<String, List<Tuple<Long, MqttMessage>>>> result2Map =
          result2.getBrokerAndTopicMqttMessageMap();
      Assert.assertEquals(1, result2Map.size());

      final Map<String, List<Tuple<Long, MqttMessage>>> result2BrokerMap = result2Map.get(MqttUtils.BROKER_URI);
      Assert.assertEquals(3, result2BrokerMap.size());
      final List<Tuple<Long, MqttMessage>> topic1Events2 = result2BrokerMap.get(topic1);
      Assert.assertEquals(1, topic1Events2.size());
      final List<Tuple<Long, MqttMessage>> topic2Events2 = result2BrokerMap.get(topic2);
      Assert.assertEquals(2, topic2Events2.size());
      final List<Tuple<Long, MqttMessage>> topic3Events2 = result2BrokerMap.get(topic3);
      Assert.assertEquals(1, topic3Events2.size());

      // Remove events from the server using the checkpoint method,
      // and check if it has been properly removed by using the replay method.
      EventReplayUtils.removeOnCheckpoint(localAddress, replayPortNum, 5L);
      Thread.sleep(500);
      final EventReplayResult result3 = EventReplayUtils.replay(localAddress, replayPortNum);
      final Map<String, Map<String, List<Tuple<Long, MqttMessage>>>> result3Map =
          result3.getBrokerAndTopicMqttMessageMap();
      System.out.println(result3Map);
      Assert.assertEquals(1, result3Map.size());
      final Map<String, List<Tuple<Long, MqttMessage>>> result3BrokerMap = result3Map.get(MqttUtils.BROKER_URI);
      Assert.assertEquals(1, result3BrokerMap.size());
      final List<Tuple<Long, MqttMessage>> topic2Events3 = result3BrokerMap.get(topic2);
      Assert.assertEquals(1, topic2Events3.size());

      // Disconnect and close all clients and servers.
      pubClient.disconnect();
      server.closeServer();
      mqttBroker.stopServer();
    } catch (final Exception e) {
      e.printStackTrace();
      throw e;
    }
  }
}