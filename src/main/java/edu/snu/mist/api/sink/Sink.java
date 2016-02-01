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
import edu.snu.mist.api.MISTStream;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.sink.builder.SinkConfiguration;

import java.util.Set;

/**
 * This interface defines necessary methods used for Sink in MIST API.
 */
public interface Sink {

  /**
   * @return The type of this Sink output
   */
  StreamType.SinkType getSinkType();

  /**
   * @return The configuration for this sink.
   */
  SinkConfiguration getSinkConfiguration();

  /**
   * @return The preceding streams of this sink.
   */
  Set<MISTStream> getPrecedingStreams();

  /**
   * @return A query which contains this sink.
   */
  MISTQuery getQuery();
}
