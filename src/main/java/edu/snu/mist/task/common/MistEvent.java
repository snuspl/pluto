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
package edu.snu.mist.task.common;

/**
 * This interface represents events of Mist.
 * MistEvent can be data or watermark. e
 */
public interface MistEvent {
  /**
   * The type of events.
   */
  enum EventType {
    DATA, // DATA: actual event data stream
    WATERMARK, // WATERMARK: watermark data
  }

  boolean isData();

  /**
   * The type of event direction. Union operator should get two events: LEFT/RIGHT events.
   * One direction operator always receive LEFT event.
   */
  enum Direction {LEFT, RIGHT}
}
