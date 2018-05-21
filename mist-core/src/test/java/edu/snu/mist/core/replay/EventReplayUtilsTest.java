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

public final class EventReplayUtilsTest {

  /**
   * This test tests the subscribe, replay, and removeOnCheckpoint APIs of the EventReplayUtils class.
   */
  @Test
  public void testUtils() throws IOException, MqttException, InterruptedException {
    try {
      final int replayPortNum = 26523;
      final String localAddress = "127.0.0.1";
      final String brokerURI = MqttUtils.BROKER_URI;
      final String topic1 = "source/user/gps";
      final String topic2 = "source/user/heartbeat";
      final String topic3 = "source/user/doorClosed";
      final String payload1 = "{\"message\": \"aaa\", \"start_timestamp\": 1}";
      final String payload2 = "{\"message\": \"bbb\", \"start_timestamp\": 2}";
      final String payload3 = "{\"message\": \"ccc\", \"start_timestamp\": 3}";
      final String payload4 = "{\"message\": \"ddd\", \"start_timestamp\": 4}";
      final String payload5 = "{\"message\": \"eee\", \"start_timestamp\": 5}";
      final String payload6 = "{\"message\": \"eee\", \"start_timestamp\": 6}";
      final MockReplayServer server = new MockReplayServer(replayPortNum);
      final Server mqttBroker = MqttUtils.createMqttBroker();
      final MqttClient pubClient = new MqttClient(brokerURI, "testPubClient");

      final Thread serverThread = new Thread(new Runnable() {
        @Override
        public void run() {
          server.startServer();
        }
      });
      serverThread.start();

      // Wait for the socket to open.
      Thread.sleep(500);
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

      // Fetch all topic1 events by using the replay method.
      final EventReplayResult result1 = EventReplayUtils.replay(localAddress, replayPortNum, brokerURI, topic1);
      final List<Tuple<Long, MqttMessage>> result1Messages = result1.getMqttMessages();
      Assert.assertEquals(3, result1Messages.size());

      final Tuple<Long, MqttMessage> result1Message1 = result1Messages.get(0);
      Assert.assertEquals(1L, (long) result1Message1.getKey());
      Assert.assertEquals(payload1, result1Message1.getValue().toString());
      final Tuple<Long, MqttMessage> result1Message2 = result1Messages.get(1);
      Assert.assertEquals(2L, (long) result1Message2.getKey());
      Assert.assertEquals(payload2, result1Message2.getValue().toString());
      final Tuple<Long, MqttMessage> result1Message3 = result1Messages.get(2);
      Assert.assertEquals(3L, (long) result1Message3.getKey());
      Assert.assertEquals(payload3, result1Message3.getValue().toString());

      // Fetch all topic2 events by using the replay method.
      final EventReplayResult result2 = EventReplayUtils.replay(localAddress, replayPortNum,
          brokerURI, topic2);
      final List<Tuple<Long, MqttMessage>> result2Messages = result2.getMqttMessages();
      Assert.assertEquals(3, result2Messages.size());

      final Tuple<Long, MqttMessage> result2Message1 = result2Messages.get(0);
      Assert.assertEquals(4L, (long) result2Message1.getKey());
      Assert.assertEquals(payload4, result2Message1.getValue().toString());

      final Tuple<Long, MqttMessage> result2Message2 = result2Messages.get(1);
      Assert.assertEquals(4L, (long) result2Message2.getKey());
      Assert.assertEquals(payload4, result2Message2.getValue().toString());

      final Tuple<Long, MqttMessage> result2Message3 = result2Messages.get(2);
      Assert.assertEquals(6L, (long) result2Message3.getKey());
      Assert.assertEquals(payload6, result2Message3.getValue().toString());

      // Fetch all topic3 events by using the replay method.
      final EventReplayResult result3 = EventReplayUtils.replay(localAddress, replayPortNum,
          brokerURI, topic3);
      final List<Tuple<Long, MqttMessage>> result3Messages = result3.getMqttMessages();
      Assert.assertEquals(1, result3Messages.size());

      final Tuple<Long, MqttMessage> result3Message1 = result3Messages.get(0);
      Assert.assertEquals(5L, (long) result3Message1.getKey());
      Assert.assertEquals(payload5, result3Message1.getValue().toString());

      // Fetch all topic2 events with timestamp 3L ~ 5L by using the replay method.
      final EventReplayResult result4 = EventReplayUtils.replay(localAddress, replayPortNum,
          brokerURI, topic2, 3L, 5L);
      final List<Tuple<Long, MqttMessage>> result4Messages = result4.getMqttMessages();
      Assert.assertEquals(2, result4Messages.size());

      final Tuple<Long, MqttMessage> result4Message1 = result4Messages.get(0);
      Assert.assertEquals(4L, (long) result4Message1.getKey());
      Assert.assertEquals(payload4, result4Message1.getValue().toString());

      final Tuple<Long, MqttMessage> result4Message2 = result4Messages.get(1);
      Assert.assertEquals(4L, (long) result4Message2.getKey());
      Assert.assertEquals(payload4, result4Message2.getValue().toString());

      // Remove all events of topic1 by using the removeOnCheckpoint method.
      EventReplayUtils.removeOnCheckpoint(localAddress, replayPortNum, brokerURI, topic1);
      final EventReplayResult result5 = EventReplayUtils.replay(localAddress, replayPortNum, brokerURI, topic1);
      Assert.assertEquals(0, result5.getMqttMessages().size());

      // Remove events of topic2 with timestamp less than 5 by using the removeOnCheckpoint method.
      EventReplayUtils.removeOnCheckpoint(localAddress, replayPortNum, brokerURI, topic2, 5L);
      final EventReplayResult result6 = EventReplayUtils.replay(localAddress, replayPortNum, brokerURI, topic2);
      final List<Tuple<Long, MqttMessage>> result6Messages = result6.getMqttMessages();
      Assert.assertEquals(1, result6Messages.size());

      final Tuple<Long, MqttMessage> result6Message1 = result6Messages.get(0);
      Assert.assertEquals(6L, (long) result6Message1.getKey());
      Assert.assertEquals(payload6, result6Message1.getValue().toString());

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