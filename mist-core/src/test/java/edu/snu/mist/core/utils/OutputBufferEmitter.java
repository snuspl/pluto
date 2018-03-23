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
package edu.snu.mist.core.utils;

import edu.snu.mist.core.*;

import java.util.List;

/**
 * Output emitter which just adds input events to a list.
 */
public final class OutputBufferEmitter implements OutputEmitter {
  private final List<MistEvent> list;

  public OutputBufferEmitter(final List<MistEvent> list) {
    this.list = list;
  }

  @Override
  public void emitData(final MistDataEvent data) {
    list.add(data);
  }
  @Override
  public void emitData(final MistDataEvent data, final int index) {
    // simple output emitter does not emit data according to the index
    this.emitData(data);
  }
  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    list.add(watermark);
  }
  @Override
  public void emitCheckpoint(final MistCheckpointEvent checkpoint) {
    // do nothing
  }
}