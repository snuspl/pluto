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

import java.util.logging.Logger;

/**
 * This abstract class is for processing LEFT stream events.
 * If RIGHT events are received, it throws an exception.
 */
public abstract class OneStreamOperator extends BaseOperator {
  private static final Logger LOG = Logger.getLogger(OneStreamOperator.class.getName());

  @Override
  public void processRightData(final MistDataEvent data) {
    throw new RuntimeException("Invalid Input Type: " + this.getClass()
    + " should have processLeft event type");
  }

  @Override
  public void processRightWatermark(final MistWatermarkEvent watermark) {
    throw new RuntimeException("Invalid Input Type: " + this.getClass()
        + " should have processLeft watermark event type");
  }

  @Override
  public void processRightCheckpoint(final MistCheckpointEvent checkpoint) {
    throw new RuntimeException("Invalid Input Type: " + this.getClass()
        + " should have processLeft checkpoint event type");
  }
}