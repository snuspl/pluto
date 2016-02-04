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
package edu.snu.mist.api.serialize.avro;

import edu.snu.mist.api.SerializedType;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.api.sink.parameters.SinkSerializeInfo;
import edu.snu.mist.formats.avro.SinkInfo;
import edu.snu.mist.formats.avro.SinkTypeEnum;
import org.apache.commons.lang.SerializationUtils;

import javax.inject.Inject;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation class for SinkInfoProvider interface.
 */
public final class SinkInfoProviderImpl implements SinkInfoProvider {

  @Inject
  private SinkInfoProviderImpl() {

  }

  @Override
  public SinkInfo getSinkInfo(final Sink sink) {
    final SinkInfo.Builder sinkInfoBuilder = SinkInfo.newBuilder();
    // Sink type detection
    if (sink.getSinkType() == StreamType.SinkType.REEF_NETWORK_SINK) {
      sinkInfoBuilder.setSinkType(SinkTypeEnum.REEF_NETWORK_SOURCE);
    } else {
      throw new IllegalStateException("Sink type is illegal!");
    }
    // Serialize SinkInfo
    final SinkConfiguration sinkConf = sink.getSinkConfiguration();
    final Map<CharSequence, Object> serializedSinkConf = new HashMap<>();
    for (final String confKey : sinkConf.getConfigurationKeys()) {
      final Object value = sinkConf.getConfigurationValue(confKey);
      if (SinkSerializeInfo.getAvroSerializedTypeInfo(confKey) != SerializedType.AvroType.BYTES) {
        serializedSinkConf.put(confKey, value);
      } else {
        serializedSinkConf.put(confKey, ByteBuffer.wrap(SerializationUtils.serialize((Serializable) value)));
      }
    }
    sinkInfoBuilder.setSinkConfiguration(serializedSinkConf);
    return sinkInfoBuilder.build();
  }
}