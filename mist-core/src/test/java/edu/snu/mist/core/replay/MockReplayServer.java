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
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class MockReplayServer {

  private static final Logger LOG = Logger.getLogger(MockReplayServer.class.getName());

  /**
   * The port number of this mock replay server.
   */
  private int portNum;

  /**
   * The server socket of this server.
   */
  private ServerSocket serverSocket;

  /**
   * The map of (broker address)-(topic) as the key, and stored events as the value.
   */
  private Map<String, List<List<Object>>> brokerTopicAndEventListMap;

  /**
   * The Mqtt client to subscribe to the broker.
   */
  private MqttClient subClient;

  public MockReplayServer(final int portNum) {
    this.portNum = portNum;
    this.brokerTopicAndEventListMap = new HashMap<>();
  }

  /**
   * Method to start and run the server.
   */
  public void startServer() {
    try {
      subClient = new MqttClient(MqttUtils.BROKER_URI, "testSubClient");

      serverSocket = new ServerSocket(portNum);
      LOG.log(Level.INFO, "The mock server was successfully started.");
      while (!serverSocket.isClosed()) {
        // If the socket is "close"d by another thread, the accept() method throws a SocketException.
        final Socket client = serverSocket.accept();
        // Get input and output streams to talk to the client.
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
        final PrintWriter printWriter = new PrintWriter(client.getOutputStream(), true);

        // Parse the input.
        String request = bufferedReader.readLine(); // The request line, such as "GET /replay HTTP/1.1".
        String[] requestParam = request.split(" ");
        String url = requestParam[1];
        if (url.equals("/replay")) {
          handleReplay(bufferedReader, printWriter);
        } else if (url.equals("/checkpoint")) {
          handleCheckpoint(bufferedReader, printWriter);
        } else if (url.equals("/subscribe")) {
          handleSubscribe(bufferedReader, printWriter);
        }
        bufferedReader.close();
        printWriter.close();
      }
    } catch (final SocketException e) {
      LOG.log(Level.INFO, "The mock server was closed.");
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Method to close the server.
   */
  public void closeServer() {
    try {
      if (subClient.isConnected()) {
        subClient.disconnect();
      }
      serverSocket.close();
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * A method to handle replay requests.
   */
  private void handleReplay(final BufferedReader bufferedReader,
                            final PrintWriter printWriter) throws IOException {
    sendHeader(printWriter);

    String currentLine = bufferedReader.readLine();
    while (currentLine != null && !currentLine.equals("")) {
      currentLine = bufferedReader.readLine();
    }
    // The line after the empty line is the body.
    currentLine = bufferedReader.readLine();
    final ObjectMapper mapper = new ObjectMapper();
    final Map<String, String> contentMap = mapper.readValue(currentLine,
        new TypeReference<Map<String, String>>() {
        });
    final String brokerURIandTopic = contentMap.get("brokerURI") + "-" + contentMap.get("topic");
    final String startTimestamp = contentMap.get("startTimestamp");
    final String endTimestamp = contentMap.get("endTimestamp");

    // Send the mqtt messages according to the start and end timestamps.
    final Map<String, List<List<Object>>> eventMap = new HashMap<>();

    if (startTimestamp == null) {
      eventMap.put("result", brokerTopicAndEventListMap.get(brokerURIandTopic));
    } else {
      final List<List<Object>> allEvents = brokerTopicAndEventListMap.get(brokerURIandTopic);
      final List<List<Object>> filteredEvents = new ArrayList<>();
      for (final List<Object> event : allEvents) {
        final Long timestamp = (Long) event.get(0);
        if (timestamp >= Long.parseLong(startTimestamp)) {
          if (endTimestamp == null) {
            filteredEvents.add(event);
          } else if (timestamp <= Long.parseLong(endTimestamp)) {
            filteredEvents.add(event);
          }
        }
      }
      // Sort the events in timestamp order.
      filteredEvents.sort(new MessageComparator());

      // Emit the replay events.
      eventMap.put("result", filteredEvents);
    }

    // Output the finished JSON response.
    JSONObject.writeJSONString(eventMap, printWriter);
    printWriter.flush();
  }

  /**
   * A method to handle removeOnCheckpoint requests.
   */
  private void handleCheckpoint(final BufferedReader bufferedReader,
                                final PrintWriter printWriter) {
    try {
      sendHeader(printWriter);
      String currentLine = bufferedReader.readLine();
      while (currentLine != null && !currentLine.equals("")) {
        currentLine = bufferedReader.readLine();
      }
      // The line after the empty line is the body.
      currentLine = bufferedReader.readLine();
      final ObjectMapper mapper = new ObjectMapper();
      final Map<String, String> contentMap = mapper.readValue(currentLine,
          new TypeReference<Map<String, String>>() {
          });
      final String brokerURIandTopic = contentMap.get("brokerURI") + "-" + contentMap.get("topic");
      final String checkpointTimestamp = contentMap.get("timestamp");

      final List<List<Object>> eventList = brokerTopicAndEventListMap.get(brokerURIandTopic);
      if (eventList != null) {
        final List<List<Object>> remainingEventList = new ArrayList<>();
        for (final List<Object> event : eventList) {
          final Long eventTimestamp = (Long) event.get(0);
          if (checkpointTimestamp != null && eventTimestamp >= Long.parseLong(checkpointTimestamp)) {
            remainingEventList.add(event);
          }
        }
        if (remainingEventList.size() == 0) {
          brokerTopicAndEventListMap.remove(brokerURIandTopic);
        } else {
          brokerTopicAndEventListMap.put(brokerURIandTopic, remainingEventList);
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * A method to handle subscribe requests.
   */
  private void handleSubscribe(final BufferedReader bufferedReader,
                               final PrintWriter printWriter) {
    try {
      sendHeader(printWriter);
      String currentLine = bufferedReader.readLine();
      while (currentLine != null && !currentLine.equals("")) {
        currentLine = bufferedReader.readLine();
      }
      // The line after the empty line is the body.
      currentLine = bufferedReader.readLine();
      final ObjectMapper mapper = new ObjectMapper();
      final Map<String, String> contentMap = mapper.readValue(currentLine,
          new TypeReference<Map<String, String>>() { });

      if (!subClient.isConnected()) {
        subClient.connect();
        subClient.setCallback(new TestMqttSubscriber());
      }
      subClient.subscribe(contentMap.get("topic"));
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Add an event to this replay server.
   */
  private void addEvent(final Long timestamp,
                        final String topic,
                        final String mqttMessage) {
    final List<Object> newEvent = new ArrayList<>();
    newEvent.add(timestamp);
    final StringBuilder message = new StringBuilder();
    message.append("{\"message\": ");
    message.append("\"" + mqttMessage + "\", ");
    message.append("\"start_timestamp\": ");
    message.append(timestamp.toString() + "}");
    newEvent.add(message.toString());
    final String brokerURIandTopic = MqttUtils.BROKER_URI + "-" + topic;
    List<List<Object>> eventList =
        brokerTopicAndEventListMap.getOrDefault(brokerURIandTopic, new ArrayList<>());
    eventList.add(newEvent);
    brokerTopicAndEventListMap.put(brokerURIandTopic, eventList);
  }

  /**
   * Returns true if this replay server is closed.
   */
  public boolean isClosed() {
    return serverSocket.isClosed();
  }

  /**
   * Send a header that indicates success.
   */
  private void sendHeader(final PrintWriter printWriter) {
    final StringBuilder header = new StringBuilder();
    header.append("HTTP/1.1 200 OK\n");
    header.append("Content-Type: application/json\n");
    printWriter.write(header.toString());
    printWriter.println();
  }

  /**
   * Mqtt subscriber that is used in the sink test.
   */
  private final class TestMqttSubscriber implements MqttCallback {
    @Override
    public void connectionLost(final Throwable cause) {
      // do nothing
    }

    @Override
    public void messageArrived(final String topic, final MqttMessage message) throws Exception {
      final ObjectMapper mapper = new ObjectMapper();
      final Map<String, String> payloadMap = mapper.readValue(new String(message.getPayload()),
          new TypeReference<Map<String, String>>() { });
      final Long timestamp = Long.parseLong(payloadMap.get("start_timestamp"));
      final String stringMessage = payloadMap.get("message");
      addEvent(timestamp, topic, stringMessage);
    }

    @Override
    public void deliveryComplete(final IMqttDeliveryToken token) {
      // do nothing
    }
  }

  /**
   * Comparator for sorting events in timestamp order.
   */
  private final class MessageComparator implements Comparator<List<Object>> {
    @Override
    public int compare(final List<Object> a1, final List<Object> a2) {
      final Long timestamp1 = (Long) a1.get(0);
      final Long timestamp2 = (Long) a2.get(0);
      return timestamp1 > timestamp2 ? 1 : (timestamp1 < timestamp2 ? -1 : 0);
    }
  }
}
