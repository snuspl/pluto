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

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.formats.avro.SinkTypeEnum;

/**
 * TextSocketSink class.
 */
public final class TextSocketSink extends BaseSink {

  public TextSocketSink(final StreamType.SinkType sinkType,
                        final SinkConfiguration sinkConfiguration) {
    super(sinkType, sinkConfiguration);
  }

  @Override
  public SinkTypeEnum getSinkTypeEnum() {
    return SinkTypeEnum.TEXT_SOCKET_SINK;
  }
}
