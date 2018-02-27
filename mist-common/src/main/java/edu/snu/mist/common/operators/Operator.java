/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistCheckpointEvent;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.OutputEmittable;

/**
 * This is an interface of mist physical operator which runs actual computation.
 * Operator receives an input, does computation, and emits an output to OutputEmitter.
 */
public interface Operator extends OutputEmittable {

  /**
   * Process data of left upstream.
   * @param data data
   */
  void processLeftData(final MistDataEvent data);

  /**
   * Process data of right upstream.
   * @param data data
   */
  void processRightData(final MistDataEvent data);

  /**
   * Process watermark of left upstream.
   * @param watermark watermark
   */
  void processLeftWatermark(final MistWatermarkEvent watermark);

  /**
   * Process watermark of right upstream.
   * @param watermark watermark
   */
  void processRightWatermark(final MistWatermarkEvent watermark);

  /**
   * Process watermark of left upstream.
   * @param checkpointEvent checkpoint event
   */
  void processLeftCheckpoint(final MistCheckpointEvent checkpointEvent);

  /**
   * Process watermark of right upstream.
   * @param checkpointEvent checkpoint event
   */
  void processRightCheckpoint(final MistCheckpointEvent checkpointEvent);
}