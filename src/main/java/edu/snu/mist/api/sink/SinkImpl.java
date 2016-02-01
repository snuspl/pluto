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

import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryImpl;
import edu.snu.mist.api.MISTStream;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.sink.builder.SinkConfiguration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The basic implementation class for Sink.
 */
public final class SinkImpl implements Sink {

  /**
   * A set of the previous streams which are right before this sink.
   */
  private final Set<MISTStream> precedingStreams;

  /**
   * The type of this sink.
   */
  private final StreamType.SinkType sinkType;

  /**
   * The value for sink configuration.
   */
  private final SinkConfiguration sinkConfiguration;

  public SinkImpl(final Set<MISTStream> precedingStreams,
                  final StreamType.SinkType sinkType,
                  final SinkConfiguration sinkConfiguration) {
    this.precedingStreams = precedingStreams;
    this.sinkType = sinkType;
    this.sinkConfiguration = sinkConfiguration;
  }

  public SinkImpl(final MISTStream precedingStream,
                  final StreamType.SinkType sinkType,
                  final SinkConfiguration sinkConfiguration) {
    this.precedingStreams = new HashSet<>(Arrays.asList(precedingStream));
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
   * @return A set of the preceding streams
   */
  @Override
  public Set<MISTStream> getPrecedingStreams() {
    return this.precedingStreams;
  }

  /**
   * @return A query which contains this sink.
   */
  @Override
  public MISTQuery getQuery() {
    return new MISTQueryImpl(this);
  }
}
