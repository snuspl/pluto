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

import edu.snu.mist.api.sink.builder.TextSocketSinkConfiguration;
import edu.snu.mist.api.sources.builder.*;
import org.apache.reef.io.Tuple;

/**
 * This class contains necessary parameters for API testing.
 */
public final class APITestParameters {

  private APITestParameters() {
    // Do nothing here
  }

  public static final TextSocketSourceConfiguration LOCAL_TEXT_SOCKET_SOURCE_CONF =
      TextSocketSourceConfiguration.newBuilder()
      .setHostAddress("localhost")
      .setHostPort(13666)
      .build();

  public static final TextSocketSourceConfiguration LOCAL_TEXT_SOCKET_EVENTTIME_SOURCE_CONF =
      TextSocketSourceConfiguration.newBuilder()
      .setHostAddress("localhost")
      .setHostPort(13666)
      .setTimestampExtractionFunction(input -> new Tuple<>(input.split(":")[0],
          Long.parseLong(input.split(":")[1])))
      .build();

  public static final PunctuatedWatermarkConfiguration<String> PUNCTUATED_WATERMARK_CONF =
      PunctuatedWatermarkConfiguration.<String>newBuilder()
          .setWatermarkPredicate(input -> input.split(":")[0].equals("Watermark"))
          .setParsingWatermarkFunction(input -> Long.parseLong(input.split(":")[1]))
          .build();

  public static final TextSocketSinkConfiguration LOCAL_TEXT_SOCKET_SINK_CONF =
      TextSocketSinkConfiguration.newBuilder()
      .setHostAddress("localhost")
      .setHostPort(13667)
      .build();
}