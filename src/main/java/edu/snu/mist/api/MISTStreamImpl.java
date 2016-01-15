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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The basic implementation class for MISTStream.
 */
public class MISTStreamImpl<OUT> implements MISTStream<OUT> {

  /**
   * The type of this stream (continuous or windowed).
   */
  private final StreamType.BasicType streamType;
  /**
   * A set of the previous streams which are right before this stream. It can be more than one in case of
   * join, union, or other transformations which get multiple input streams.
   */
  private final Set<MISTStream> inputStreams;

  public MISTStreamImpl(final StreamType.BasicType streamType, final MISTStream inputStream) {
    this.streamType = streamType;
    this.inputStreams = new HashSet<>(Arrays.asList(inputStream));
  }

  public MISTStreamImpl(final StreamType.BasicType streamType, final Set<MISTStream> inputStreams) {
    this.streamType = streamType;
    this.inputStreams = inputStreams;
  }

  public MISTStreamImpl(final StreamType.BasicType streamType) {
    this.streamType = streamType;
    this.inputStreams = null;
  }

  @Override
  public StreamType.BasicType getBasicType() {
    return streamType;
  }

  /**
   * @return A set of the preceding streams
   */
  @Override
  public Set<MISTStream> getInputStreams() {
    return inputStreams;
  }
}
