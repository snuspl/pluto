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
package edu.snu.mist.common.parameters;

/**
 * This class contains the list of necessary parameters for PunctuatedWatermarkConfiguration.
 */
public final class PunctuatedWatermarkParameters {

  private PunctuatedWatermarkParameters() {
    // Not called.
  }

  /**
   * The MISTPredicate that judges whether the input data means punctuated watermark or not.
   */
  public static final String WATERMARK_PREDICATE = "WatermarkPredicate";

  /**
   * The MISTFunction that parses timestamp from input indicating punctuated watermark.
   */
  public static final String PARSING_TIMESTAMP_FROM_WATERMARK = "ParsingTimestampFromWatermark";
}