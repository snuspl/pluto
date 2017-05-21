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
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.Direction;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is used for setting the OutputEmitter of Deactivated Sources.
 */
final class DeactivatedSourceOutputEmitter implements OutputEmitter {

  private static final Logger LOG = Logger.getLogger(DeactivatedSourceOutputEmitter.class.getName());

  /**
   * The query Id that the deactivated source came from.
   */
  private final String queryId;

  /**
   * The map of execution vertices which has union operators that have deactivated upstreams.
   */
  private final Map<ExecutionVertex, MISTEdge> needWatermarkVertices;

  /**
   * The GroupSourceManager of this source's group.
   */
  private final GroupSourceManager groupSourceManager;

  /**
   * The deactivated source itself.
   */
  private final PhysicalSource source;

  /**
   * The id of the deactivated source.
   */
  private final String sourceID;

  /**
   * This indicates if emitting watermarks must be stopped.
   */
  private boolean stoppedEmitWatermark;

  public DeactivatedSourceOutputEmitter(final String queryId,
                                        final Map<ExecutionVertex, MISTEdge> needWatermarkVertices,
                                        final GroupSourceManager groupSourceManager,
                                        final PhysicalSource source) {
    this.queryId = queryId;
    this.needWatermarkVertices = needWatermarkVertices;
    this.groupSourceManager = groupSourceManager;
    this.source = source;
    this.sourceID = source.getIdentifier();
    this.stoppedEmitWatermark = false;
  }

  @Override
  public void emitData(final MistDataEvent data) {
    try {
      // Stop emitting watermarks.
      stoppedEmitWatermark = true;
      // Activate the source, because there was an input to this deactivated source.
      groupSourceManager.activateBasedOnSource(queryId, sourceID);
      // Emit watermarks to the new emitter.
      source.getEventGenerator().getOutputEmitter().emitData(data);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "An exception occurred while activating the source with ID {0} with a data event.",
          new Object[] {sourceID});
    }
  }

  @Override
  public void emitData(final MistDataEvent data, final int index) {
    // source output emitter does not emit data according to the index
    this.emitData(data);
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    if (!stoppedEmitWatermark) {
      for (final Map.Entry<ExecutionVertex, MISTEdge> nextQuery :
          needWatermarkVertices.entrySet()) {
        final Direction direction = nextQuery.getValue().getDirection();
        ((OperatorChain) nextQuery.getKey()).addNextEvent(watermark, direction);
      }
    }
  }
}
