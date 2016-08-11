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

package edu.snu.mist.api.sink;

import edu.snu.mist.api.SerializedType;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.api.sink.parameters.SinkSerializeInfo;
import edu.snu.mist.formats.avro.SinkInfo;
import edu.snu.mist.formats.avro.SinkTypeEnum;
import edu.snu.mist.formats.avro.Vertex;
import edu.snu.mist.formats.avro.VertexTypeEnum;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * The base class for sink.
 * @param <T> the type of sink data
 */
public abstract class BaseSink<T> implements Sink {

  /**
   * The type of this sink.
   */
  protected final StreamType.SinkType sinkType;

  /**
   * The value for sink configuration.
   */
  protected final SinkConfiguration<T> sinkConfiguration;

  public BaseSink(final StreamType.SinkType sinkType,
                  final SinkConfiguration<T> sinkConfiguration) {
    this.sinkType = sinkType;
    this.sinkConfiguration = sinkConfiguration;
  }

  /**
   * @return The type of the sink stream
   */
  @Override
  public StreamType.SinkType getSinkType() {
    return this.sinkType;
  }

  /**
   * @return The SinkConfiguration set for this stream
   */
  @Override
  public SinkConfiguration getSinkConfiguration() {
    return this.sinkConfiguration;
  }

  /**
   * Get avro sink type enum.
   */
  protected abstract SinkTypeEnum getSinkTypeEnum();

  @Override
  public Vertex getSerializedVertex() {
    final Vertex.Builder vertexBuilder = Vertex.newBuilder();
    vertexBuilder.setVertexType(VertexTypeEnum.SINK);
    final SinkInfo.Builder sinkInfoBuilder = SinkInfo.newBuilder();
    sinkInfoBuilder.setSinkType(getSinkTypeEnum());

    // Serialize SinkInfo
    final Map<CharSequence, Object> serializedSinkConf = new HashMap<>();
    for (final String confKey : sinkConfiguration.getConfigurationKeys()) {
      final Object value = sinkConfiguration.getConfigurationValue(confKey);
      if (SinkSerializeInfo.getAvroSerializedTypeInfo(confKey) != SerializedType.AvroType.BYTES) {
        serializedSinkConf.put(confKey, value);
      } else {
        serializedSinkConf.put(confKey, ByteBuffer.wrap(SerializationUtils.serialize((Serializable) value)));
      }
    }

    sinkInfoBuilder.setSinkConfiguration(serializedSinkConf);
    vertexBuilder.setAttributes(sinkInfoBuilder.build());
    return vertexBuilder.build();
  }
}
