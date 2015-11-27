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
package edu.snu.mist.api.sources;

import edu.snu.mist.api.ContinuousStreamImpl;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.sources.builder.SourceConfiguration;

/**
 * Stream interface for streams created from various stream sources.
 */
public abstract class SourceStream<T> extends ContinuousStreamImpl<T> {
  /**
   * The value for source configuration.
   */
  private final SourceConfiguration sourceConfiguration;
  /**
   * The type of this source.
   */
  private final StreamType.SourceType sourceType;

  SourceStream(final StreamType.SourceType sourceType, final SourceConfiguration sourceConfiguration) {
    super(StreamType.ContinuousType.SOURCE);
    this.sourceType = sourceType;
    this.sourceConfiguration = sourceConfiguration;
  }

  /**
   * @return The type of the current stream source (ex: HDFS, NCS, ...)
   */
  public StreamType.SourceType getSourceType() {
    return sourceType;
  }

  /**
   * @return The SourceConfiguration set for this stream
   */
  public SourceConfiguration getSourceConfiguration() {
    return this.sourceConfiguration;
  }
}