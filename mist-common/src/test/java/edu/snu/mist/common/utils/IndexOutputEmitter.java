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
package edu.snu.mist.common.utils;

import edu.snu.mist.common.*;
import org.apache.reef.io.Tuple;

import java.util.List;

/**
 * Simple output emitter which records events and indices to the list.
 */
public final class IndexOutputEmitter implements OutputEmitter {
  private final List<Tuple<MistEvent, Integer>> list;

  public IndexOutputEmitter(final List<Tuple<MistEvent, Integer>> list) {
    this.list = list;
  }

  @Override
  public void emitData(final MistDataEvent data) {
    this.emitData(data, 0);
  }
  @Override
  public void emitData(final MistDataEvent data, final int index) {
    list.add(new Tuple<>(data, index));
  }
  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    list.add(new Tuple<>(watermark, 0));
  }
}