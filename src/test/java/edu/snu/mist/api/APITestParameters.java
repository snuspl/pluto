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

import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfigurationBuilderImpl;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.builder.*;
import edu.snu.mist.api.sources.parameters.PunctuatedWatermarkParameters;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import org.apache.reef.io.Tuple;

/**
 * This class contains necessary parameters for API testing.
 */
public final class APITestParameters {

  private APITestParameters() {
    // Do nothing here
  }

  public static final SourceConfiguration LOCAL_TEXT_SOCKET_SOURCE_CONF =
      new TextSocketSourceConfigurationBuilder()
      .set(TextSocketSourceParameters.SOCKET_HOST_ADDRESS, "localhost")
      .set(TextSocketSourceParameters.SOCKET_HOST_PORT, 13666)
      .build();

  public static final SourceConfiguration LOCAL_TEXT_SOCKET_EVENTTIME_SOURCE_CONF =
      new TextSocketSourceConfigurationBuilder()
      .set(TextSocketSourceParameters.SOCKET_HOST_ADDRESS, "localhost")
      .set(TextSocketSourceParameters.SOCKET_HOST_PORT, 13666)
      .set(TextSocketSourceParameters.TIMESTAMP_EXTRACTION_FUNCTION,
          (MISTFunction) (input -> new Tuple<>(input.toString().split(":")[0],
              Long.parseLong(input.toString().split(":")[1]))))
      .build();

  public static final PunctuatedWatermarkConfiguration PUNCTUTATED_WATERMARK_CONF =
      new PunctuatedWatermarkConfigurationBuilder()
          .set(PunctuatedWatermarkParameters.WATERMARK_PREDICATE,
              (MISTFunction) input -> input.toString().split(":")[0].equals("Watermark"))
          .set(PunctuatedWatermarkParameters.PARSING_TIMESTAMP_FROM_WATERMARK,
              (MISTFunction) input -> Long.parseLong(input.toString().split(":")[1]))
          .build();

  public static final SinkConfiguration LOCAL_TEXT_SOCKET_SINK_CONF = new TextSocketSinkConfigurationBuilderImpl()
      .set(TextSocketSinkParameters.SOCKET_HOST_ADDRESS, "localhost")
      .set(TextSocketSinkParameters.SOCKET_HOST_PORT, 13667)
      .build();
}