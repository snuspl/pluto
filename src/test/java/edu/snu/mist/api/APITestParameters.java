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
package edu.snu.mist.api;

import edu.snu.mist.api.sink.builder.REEFNetworkSinkConfigurationBuilderImpl;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.api.sink.builder.TextKafkaSinkConfigurationBuilderImpl;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfigurationBuilderImpl;
import edu.snu.mist.api.sink.parameters.REEFNetworkSinkParameters;
import edu.snu.mist.api.sink.parameters.TextKafkaSinkParameters;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.builder.REEFNetworkSourceConfigurationBuilderImpl;
import edu.snu.mist.api.sources.builder.SourceConfiguration;
import edu.snu.mist.api.sources.builder.TextKafkaSourceConfigurationBuilderImpl;
import edu.snu.mist.api.sources.builder.TextSocketSourceConfigurationBuilderImpl;
import edu.snu.mist.api.sources.parameters.REEFNetworkSourceParameters;
import edu.snu.mist.api.sources.parameters.TextKafkaSourceParameters;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import org.apache.reef.wake.remote.impl.StringCodec;

/**
 * This class contains necessary parameters for API testing.
 */
public final class APITestParameters {

  private APITestParameters() {
    // Do nothing here
  }

  public static final SourceConfiguration LOCAL_REEF_NETWORK_SOURCE_CONF =
      new REEFNetworkSourceConfigurationBuilderImpl()
      .set(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME, "localhost")
      .set(REEFNetworkSourceParameters.NAME_SERVICE_PORT, 13666)
      .set(REEFNetworkSourceParameters.CONNECTION_ID, "TestConn")
      .set(REEFNetworkSourceParameters.SENDER_ID, "TestSender")
      .set(REEFNetworkSourceParameters.CODEC, StringCodec.class)
      .build();

  public static final SourceConfiguration LOCAL_TEXT_SOCKET_SOURCE_CONF =
      new TextSocketSourceConfigurationBuilderImpl()
      .set(TextSocketSourceParameters.SOCKET_HOST_ADDRESS, "localhost")
      .set(TextSocketSourceParameters.SOCKET_HOST_PORT, 13667)
      .build();

  public static final SourceConfiguration LOCAL_TEXT_KAFKA_SOURCE_CONF =
      new TextKafkaSourceConfigurationBuilderImpl()
      .set(TextKafkaSourceParameters.KAFKA_HOST_ADDRESS, "localhost")
      .set(TextKafkaSourceParameters.KAFKA_HOST_PORT, 13668)
      .set(TextKafkaSourceParameters.KAFKA_TOPIC_NAME, "test_source_topic")
      .set(TextKafkaSourceParameters.KAFKA_NUM_PARTITION, 1)
      .build();

  public static final SinkConfiguration LOCAL_REEF_NETWORK_SINK_CONF = new REEFNetworkSinkConfigurationBuilderImpl()
      .set(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME, "localhost")
      .set(REEFNetworkSinkParameters.NAME_SERVICE_PORT, 13686)
      .set(REEFNetworkSinkParameters.CONNECTION_ID, "TestConn")
      .set(REEFNetworkSinkParameters.RECEIVER_ID, "TestReceiver")
      .set(REEFNetworkSinkParameters.CODEC, StringCodec.class)
      .build();

  public static final SinkConfiguration LOCAL_TEXT_SOCKET_SINK_CONF = new TextSocketSinkConfigurationBuilderImpl()
      .set(TextSocketSinkParameters.SOCKET_HOST_ADDRESS, "localhost")
      .set(TextSocketSinkParameters.SOCKET_HOST_PORT, 13687)
      .build();

  public static final SinkConfiguration LOCAL_TEXT_KAFKA_SINK_CONF = new TextKafkaSinkConfigurationBuilderImpl()
      .set(TextKafkaSinkParameters.KAFKA_HOST_ADDRESS, "localhost")
      .set(TextKafkaSinkParameters.KAFKA_HOST_PORT, 13688)
      .set(TextKafkaSinkParameters.KAFKA_TOPIC_NAME, "text_sink_topic")
      .set(TextKafkaSinkParameters.KAFKA_NUM_PARTITION, 1)
      .build();
}