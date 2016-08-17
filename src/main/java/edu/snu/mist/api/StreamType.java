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

/**
 * This class contains enum variables used for distinguishing the type of MISTStreams.
 */
public final class StreamType {

  private StreamType() {
    // Not called
  }

  /**
   * The basic type of the MISTStream. It determines whether the type is continuous or windowed stream.
   */
  public static enum BasicType {CONTINUOUS, WINDOWED}

  /**
   * The type of the ContinuousStream. It can be whether source or operator stream.
   */
  public static enum ContinuousType {SOURCE, OPERATOR}

  /**
   * The type of source stream.
   */
  public static enum SourceType {TEXT_SOCKET_SOURCE}

  /**
   * The type of source watermark.
   */
  public static enum WatermarkType {PERIODIC, PUNCTUATED}

  /**
   * The type of sink stream.
   */
  public static enum SinkType {TEXT_SOCKET_SINK}

  /**
   * The type of operator stream.
   */
  public static enum OperatorType {FILTER, FLAT_MAP, MAP, REDUCE_BY_KEY, REDUCE_BY_KEY_WINDOW,
    APPLY_STATEFUL, UNION, AGGREGATE_WINDOW}

  /**
   * The type of stream direction. Union operator should get two streams: LEFT/RIGHT.
   * One direction operator always receive LEFT stream.
   */
  public static enum Direction {LEFT, RIGHT}
}