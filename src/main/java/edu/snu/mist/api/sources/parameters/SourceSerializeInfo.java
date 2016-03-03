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
package edu.snu.mist.api.sources.parameters;

import com.google.common.collect.ImmutableMap;
import edu.snu.mist.api.SerializedType;

import java.util.Map;

/**
 * This class contains information about source serialization.
 */
public final class SourceSerializeInfo {

  private SourceSerializeInfo() {
    // Not called.
  }

  /**
   * The information about avro-serialized types for sink configurtaion value.
   */
  private static Map<String, SerializedType.AvroType> avroSerializedTypes =
      ImmutableMap.<String, SerializedType.AvroType>builder()
          .put(REEFNetworkSourceParameters.CODEC, SerializedType.AvroType.BYTES)
          .put(REEFNetworkSourceParameters.CONNECTION_ID, SerializedType.AvroType.STRING)
          .put(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME, SerializedType.AvroType.STRING)
          .put(REEFNetworkSourceParameters.NAME_SERVICE_PORT, SerializedType.AvroType.INT)
          .put(REEFNetworkSourceParameters.SENDER_ID, SerializedType.AvroType.STRING)
          .put(TextSocketSourceParameters.SOCKET_HOST_ADDRESS, SerializedType.AvroType.STRING)
          .put(TextSocketSourceParameters.SOCKET_HOST_PORT, SerializedType.AvroType.INT)
          .put(TextKafkaSourceParameters.KAFKA_HOST_ADDRESS, SerializedType.AvroType.STRING)
          .put(TextKafkaSourceParameters.KAFKA_HOST_PORT, SerializedType.AvroType.INT)
          .put(TextKafkaSourceParameters.KAFKA_TOPIC_NAME, SerializedType.AvroType.STRING)
          .put(TextKafkaSourceParameters.KAFKA_NUM_PARTITION, SerializedType.AvroType.INT)
          .build();

  /**
   * @return serialized type information of source.
   */
  public static SerializedType.AvroType getAvroSerializedTypeInfo(final String key) {
    return avroSerializedTypes.get(key);
  }
}