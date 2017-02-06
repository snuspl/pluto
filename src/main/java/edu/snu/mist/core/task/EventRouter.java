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
package edu.snu.mist.core.task;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * EventRouter receives all of the events from sources and operators
 * and routes the events to the appropriate downstream operators or sinks.
 */
@DefaultImplementation(EventRouterImpl.class)
interface EventRouter {

  /**
   * Emits data events to downstream vertices.
   * @param data data event
   * @param context event context
   */
  void emitData(MistDataEvent data, EventContext context);

  /**
   * Emits watermark events to downstream vertices.
   * @param watermark watermark event
   * @param context event context
   */
  void emitWatermark(MistWatermarkEvent watermark, EventContext context);
}