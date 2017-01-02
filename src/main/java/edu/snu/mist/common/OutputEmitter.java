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
package edu.snu.mist.common;

/**
 * This interface forwards the emitted output as a input of next operators.
 */
public interface OutputEmitter {

  /**
   * Emits an output as an input of next operators.
   * @param data data
   */
  void emitData(MistDataEvent data);

  /**
   * Emits an output as an input of next operators.
   * @param watermark watermark
   */
  void emitWatermark(MistWatermarkEvent watermark);
}
