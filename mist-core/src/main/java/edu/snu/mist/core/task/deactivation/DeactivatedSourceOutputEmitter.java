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
package edu.snu.mist.core.task.deactivation;

import edu.snu.mist.common.MistCheckpointEvent;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.ExecutionVertex;
import edu.snu.mist.core.task.PhysicalSource;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
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
   * Union operators may dependent on multiple sources that are active.
   * Even if one of those sources are deactivated, they should still be able to process data,
   * because they have incoming data events from active sources.
   * Currently, the union operator requires watermarks from both the left and right incoming edges
   * in order to process the data correctly.
   * Therefore, for union operators that still have active sources after the deactivation of a source,
   * they should receive watermarks from the deactivated source and are collected in this map.
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
  private volatile boolean stoppedEmitWatermark;

  /**
   * This queue contains Watermarks that were stopped from being emitted during activation.
   */
  private Queue<MistWatermarkEvent> stoppedWatermarks;

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
    this.stoppedWatermarks = new LinkedBlockingQueue<>();
  }

  @Override
  public void emitData(final MistDataEvent data) {
    try {
      // Stop emitting watermarks.
      stoppedEmitWatermark = true;
      // Activate the source, because there was an input to this deactivated source.
      groupSourceManager.activateBasedOnSource(queryId, sourceID);
      // Retrieve the new SourceOutputEmitter.
      final OutputEmitter newSourceOutputEmitter = source.getEventGenerator().getOutputEmitter();
      // Emit the data that activated this source.
      newSourceOutputEmitter.emitData(data);
      // Emit watermarks to the new emitter.
      while (!stoppedWatermarks.isEmpty()) {
        newSourceOutputEmitter.emitWatermark(stoppedWatermarks.remove());
      }
    } catch (final IOException e) {
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
    /* TODO: Remove
    if (!stoppedEmitWatermark) {
      for (final Map.Entry<ExecutionVertex, MISTEdge> nextQuery :
          needWatermarkVertices.entrySet()) {
        final Direction direction = nextQuery.getValue().getDirection();
        ((OperatorChain) nextQuery.getKey()).addNextEvent(watermark, direction);
      }
    } else {
      stoppedWatermarks.add(watermark);
    }

    */
  }

  @Override
  public void emitCheckpoint(final MistCheckpointEvent mistCheckpointEvent) {
    // do nothing
  }
}
