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
import edu.snu.mist.api.sources.SourceStream;
import edu.snu.mist.api.sources.builder.SourceConfiguration;
import edu.snu.mist.api.sources.parameters.SourceSerializeInfo;
import edu.snu.mist.formats.avro.SourceInfo;
import edu.snu.mist.formats.avro.SourceTypeEnum;
import org.apache.commons.lang.SerializationUtils;

import javax.inject.Inject;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation class for SourceInfoProvider.
 */
public final class SourceInfoProviderImpl implements SourceInfoProvider {

  @Inject
  SourceInfoProviderImpl() {

  }

  @Override
  public SourceInfo getSourceInfo(final SourceStream sourceStream) {
    final SourceInfo.Builder sourceInfoBuilder = SourceInfo.newBuilder();
    // Source type detection
    if (sourceStream.getSourceType() == StreamType.SourceType.REEF_NETWORK_SOURCE) {
      sourceInfoBuilder.setSourceType(SourceTypeEnum.REEF_NETWORK_SOURCE);
    } else {
      throw new IllegalStateException("Source type is illegal!");
    }
    // Serialize SourceInfo
    final SourceConfiguration sourceConf = sourceStream.getSourceConfiguration();
    final Map<CharSequence, Object> serializedSourceConf = new HashMap<>();
    for (final String confKey: sourceConf.getConfigurationKeys()) {
      final Object value = sourceConf.getConfigurationValue(confKey);
      if (SourceSerializeInfo.getAvroSerializedTypeInfo(confKey) != SerializedType.AvroType.BYTES) {
        serializedSourceConf.put(confKey, value);
      } else {
        serializedSourceConf.put(confKey, ByteBuffer.wrap(SerializationUtils.serialize((Serializable) value)));
      }
    }
    sourceInfoBuilder.setSourceConfiguration(serializedSourceConf);
    return sourceInfoBuilder.build();
  }
}
