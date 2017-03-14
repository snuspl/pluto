/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.utils;

import edu.snu.mist.api.datastreams.configurations.PunctuatedWatermarkConfiguration;
import edu.snu.mist.api.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.WatermarkConfiguration;
import org.apache.reef.io.Tuple;

/**
 * This class contains necessary parameters.
 */
public final class TestParameters {

  private TestParameters() {
    // Do nothing here
  }

  public static final String HOST = "localhost";
  public static final int SERVER_PORT = 13666;
  public static final int SINK_PORT = 13667;
  public static final String TOPIC = "mqttTopic";

  public static final SourceConfiguration LOCAL_TEXT_SOCKET_SOURCE_CONF =
      TextSocketSourceConfiguration.newBuilder()
      .setHostAddress(HOST)
      .setHostPort(SERVER_PORT)
      .build();

  public static final SourceConfiguration LOCAL_TEXT_SOCKET_EVENTTIME_SOURCE_CONF =
      TextSocketSourceConfiguration.newBuilder()
      .setHostAddress(HOST)
      .setHostPort(SERVER_PORT)
      .setTimestampExtractionFunction(input -> new Tuple<>(input.split(":")[0],
          Long.parseLong(input.split(":")[1])))
      .build();

  public static final WatermarkConfiguration PUNCTUATED_WATERMARK_CONF =
      PunctuatedWatermarkConfiguration.<String>newBuilder()
          .setWatermarkPredicate(input -> input.split(":")[0].equals("Watermark"))
          .setParsingWatermarkFunction(input -> Long.parseLong(input.split(":")[1]))
          .build();
}