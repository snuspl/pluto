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
import edu.snu.mist.common.OutputEmitter;

/**
 * This output emitter sends events to EventRouter.
 */
final class EmitterForEventRouter implements OutputEmitter {

  private final EventContext context;
  private final EventRouter eventRouter;

  public EmitterForEventRouter(final EventContext context,
                               final EventRouter eventRouter) {
    this.context = context;
    this.eventRouter = eventRouter;
  }

  @Override
  public void emitData(final MistDataEvent data) {
    eventRouter.emitData(data, context);
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    eventRouter.emitWatermark(watermark, context);
  }
}