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
package edu.snu.mist.task.operators;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;
import edu.snu.mist.task.operators.parameters.OperatorId;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * Union operator which unifies two upstreams.
 * We suppose the upstream events are always ordered.
 *
 * This operator has two queues: leftUpstreamQueue and rightUpstreamQueue.
 * Each queue contains MistDataEvent and MistWatermarkEvent.
 * The operator drains events from the queues to the next operator until watermark timestamp.
 */
public final class UnionOperator extends TwoStreamOperator {
  private static final Logger LOG = Logger.getLogger(UnionOperator.class.getName());

  private final Queue<MistEvent> leftUpstreamQueue;
  private final Queue<MistEvent> rightUpstreamQueue;
  private long recentLeftWatermark;
  private long recentRightWatermark;

  @Inject
  private UnionOperator(@Parameter(QueryId.class) final String queryId,
                        @Parameter(OperatorId.class) final String operatorId,
                        final StringIdentifierFactory idfactory) {
    super(idfactory.getNewInstance(queryId), idfactory.getNewInstance(operatorId));
    this.leftUpstreamQueue = new LinkedBlockingQueue<>();
    this.rightUpstreamQueue = new LinkedBlockingQueue<>();
    this.recentLeftWatermark = 0L;
    this.recentRightWatermark = 0L;
  }


  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.UNION;
  }

  /**
   * Peek the timestamp of the first event.
   * @param queue queue
   * @return timestamp
   */
  private long peekTimestamp(final Queue<MistEvent> queue) {
    return queue.peek().getTimestamp();
  }

  /**
   * Emits events which have less timestamp than the required timestamp from the queue .
   * @param queue queue
   * @param timestamp timestamp
   */
  private void drainUntilTimestamp(final Queue<MistEvent> queue, final long timestamp) {
    // The events in the queue is ordered by timestamp, so just peeks one by one
    while (!queue.isEmpty() && peekTimestamp(queue) <= timestamp) {
      final MistEvent event = queue.poll();
      if (event.isData()) {
        outputEmitter.emitData((MistDataEvent)event);
      } else {
        outputEmitter.emitWatermark((MistWatermarkEvent)event);
      }
    }
  }

  /**
   * Emits events which have less timestamp than the required timestamp
   * from the leftUpstreamQueue and rightUpstreamQueue.
   * @param timestamp timestamp
   */
  private void drainUntilTimestamp(final long timestamp) {
    // The events in the queue is ordered by timestamp, so just peeks one by one
    while (!leftUpstreamQueue.isEmpty() && !rightUpstreamQueue.isEmpty()) {
      // Pick minimum timestamp from left and right queue.
      long leftTs = peekTimestamp(leftUpstreamQueue);
      long rightTs = peekTimestamp(rightUpstreamQueue);

      // End of the drain
      if (leftTs > timestamp && rightTs > timestamp) {
        return;
      }

      if (leftTs <= rightTs) {
        final MistEvent event = leftUpstreamQueue.poll();
        if (event.isData()) {
          outputEmitter.emitData((MistDataEvent)event);
        } else {
          outputEmitter.emitWatermark((MistWatermarkEvent) event);
        }
      } else {
        final MistEvent event = rightUpstreamQueue.poll();
        if (event.isData()) {
          outputEmitter.emitData((MistDataEvent)event);
        } else {
          outputEmitter.emitWatermark((MistWatermarkEvent)event);
        }
      }
    }

    // If one of the upstreamQueues is not empty, then drain events from the queue
    if (!(leftUpstreamQueue.isEmpty() && rightUpstreamQueue.isEmpty())) {
      if (leftUpstreamQueue.isEmpty()) {
        // Drain from right upstream queue.
        drainUntilTimestamp(rightUpstreamQueue, timestamp);
      } else {
        // Drain from left upstream queue.
        drainUntilTimestamp(leftUpstreamQueue, timestamp);
      }
    }
  }

  @Override
  public void processLeftData(final MistDataEvent event) {
    final long timestamp = event.getTimestamp();
    if (recentLeftWatermark > timestamp) {
      throw new RuntimeException("The upstream events should be ordered by timestamp.");
    }
    recentLeftWatermark = timestamp;
    final long minimumWatermark = Math.min(recentLeftWatermark, recentRightWatermark);
    leftUpstreamQueue.add(event);
    drainUntilTimestamp(minimumWatermark);
  }

  @Override
  public void processRightData(final MistDataEvent event) {
    final long timestamp = event.getTimestamp();
    if (recentRightWatermark > timestamp) {
      throw new RuntimeException("The upstream events should be ordered by timestamp.");
    }
    recentRightWatermark = timestamp;
    final long minimumWatermark = Math.min(recentLeftWatermark, recentRightWatermark);
    rightUpstreamQueue.add(event);
    drainUntilTimestamp(minimumWatermark);
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent event) {
    if (event.getTimestamp() < recentLeftWatermark) {
      throw new RuntimeException("The timestamp of watermark should be greater or equal " +
          "than the previous event timestamp.");
    }
    recentLeftWatermark = event.getTimestamp();
    final long minimumWatermark = Math.min(recentLeftWatermark, recentRightWatermark);
    // Drain events until this watermark
    drainUntilTimestamp(minimumWatermark);
    if (recentLeftWatermark <= recentRightWatermark) {
      outputEmitter.emitWatermark(event);
    } else {
      leftUpstreamQueue.add(event);
    }
  }

  @Override
  public void processRightWatermark(final MistWatermarkEvent event) {
    if (event.getTimestamp() < recentRightWatermark) {
      throw new RuntimeException("The timestamp of watermark should be greater or equal " +
          "than the previous event timestamp.");
    }
    recentRightWatermark = event.getTimestamp();
    final long minimumWatermark = Math.min(recentLeftWatermark, recentRightWatermark);
    // Drain events until this watermark
    drainUntilTimestamp(minimumWatermark);
    if (recentRightWatermark <= recentLeftWatermark) {
      outputEmitter.emitWatermark(event);
    } else {
      rightUpstreamQueue.add(event);
    }
  }
}