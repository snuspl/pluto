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

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.operators.OneStreamOperator;

/**
 * Test operator that just forwards inputs to outputEmitter.
 */
public final class TestOperator extends OneStreamOperator {
  public TestOperator(final String opId) {
    super(opId);
  }

  @Override
  public void processLeftData(final MistDataEvent data) {
    outputEmitter.emitData(data);
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent watermark) {
    // do nothing
  }
}