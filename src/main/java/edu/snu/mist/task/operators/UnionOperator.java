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
 * TODO: [MIST-#] Consider out-of-order upstreams in Union operator.
 */
public final class UnionOperator extends TwoStreamOperator {
  private static final Logger LOG = Logger.getLogger(UnionOperator.class.getName());

  private final Queue<MistDataEvent> leftUpstreamDataQueue;
  private final Queue<MistDataEvent> rightUpstreamDataQueue;
  private long recentLeftWatermark;
  private long recentRightWatermark;

  @Inject
  private UnionOperator(@Parameter(QueryId.class) final String queryId,
                        @Parameter(OperatorId.class) final String operatorId,
                        final StringIdentifierFactory idfactory) {
    super(idfactory.getNewInstance(queryId), idfactory.getNewInstance(operatorId));
    this.leftUpstreamDataQueue = new LinkedBlockingQueue<>();
    this.rightUpstreamDataQueue = new LinkedBlockingQueue<>();
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
  private long peekTimestamp(final Queue<MistDataEvent> queue) {
    return queue.peek().getTimestamp();
  }

  /**
   * Emits events which have less timestamp than the required timestamp from the queue .
   * @param queue queue
   * @param timestamp timestamp
   */
  private void drainUntilTimestamp(final Queue<MistDataEvent> queue, final long timestamp) {
    // The events in the queue is ordered by timestamp, so just peeks one by one
    while (!queue.isEmpty() && queue.peek().getTimestamp() <= timestamp) {
      final MistDataEvent input = queue.poll();
      outputEmitter.emitData(input);
    }
  }

  /**
   * Emits events which have less timestamp than the required timestamp
   * from the leftUpstreamDataQueue and rightUpstreamDataQueue.
   * @param timestamp timestamp
   */
  private void drainUntilTimestamp(final long timestamp) {
    // The events in the queue is ordered by timestamp, so just peeks one by one
    while (!leftUpstreamDataQueue.isEmpty() && !rightUpstreamDataQueue.isEmpty()) {
      // Pick minimum timestamp from left and right queue.
      long leftTs = peekTimestamp(leftUpstreamDataQueue);
      long rightTs = peekTimestamp(rightUpstreamDataQueue);

      // End of the drain
      if (leftTs > timestamp && rightTs > timestamp) {
        return;
      }

      if (leftTs <= rightTs) {
        final MistDataEvent input = leftUpstreamDataQueue.poll();
        outputEmitter.emitData(input);
      } else {
        final MistDataEvent input = rightUpstreamDataQueue.poll();
        outputEmitter.emitData(input);
      }
    }

    // If one of the upstreamQueues is not empty, then drain events from the queue
    if (!(leftUpstreamDataQueue.isEmpty() && rightUpstreamDataQueue.isEmpty())) {
      if (leftUpstreamDataQueue.isEmpty()) {
        // Drain from right upstream queue.
        drainUntilTimestamp(rightUpstreamDataQueue, timestamp);
      } else {
        // Drain from left upstream queue.
        drainUntilTimestamp(leftUpstreamDataQueue, timestamp);
      }
    }
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    final long timestamp = input.getTimestamp();
    // TODO: [MIST-#] : Consider late events in which the timestamp is less than watermark.
    // For simplicity, we drop events if the timestamp is lower than watermark!
    if (recentLeftWatermark < timestamp) {
      recentLeftWatermark = timestamp;
      final long minimumWatermark = Math.min(recentLeftWatermark, recentRightWatermark);
      leftUpstreamDataQueue.add(input);
      drainUntilTimestamp(minimumWatermark);
    }
  }

  @Override
  public void processRightData(final MistDataEvent input) {
    final long timestamp = input.getTimestamp();
    // TODO: [MIST-#] : Consider late events in which the timestamp is less than watermark.
    // For simplicity, we drop events if the timestamp is lower than watermark!
    if (recentRightWatermark < timestamp) {
      recentRightWatermark = timestamp;
      final long minimumWatermark = Math.min(recentLeftWatermark, recentRightWatermark);
      rightUpstreamDataQueue.add(input);
      drainUntilTimestamp(minimumWatermark);
    }
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    recentLeftWatermark = input.getTimestamp();
    final long minimumWatermark = Math.min(recentLeftWatermark, recentRightWatermark);
    // Drain events until this watermark
    drainUntilTimestamp(minimumWatermark);
    input.setTimestamp(minimumWatermark);
    outputEmitter.emitWatermark(input);
  }

  @Override
  public void processRightWatermark(final MistWatermarkEvent input) {
    recentRightWatermark = input.getTimestamp();
    final long minimumWatermark = Math.min(recentLeftWatermark, recentRightWatermark);
    // Drain events until this watermark
    drainUntilTimestamp(minimumWatermark);
    input.setTimestamp(minimumWatermark);
    outputEmitter.emitWatermark(input);
  }
}