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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
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

  private final Queue<MistDataEvent> leftUpstreamQueue;
  private final Queue<MistDataEvent> rightUpstreamQueue;
  private final MistWatermarkEvent defaultWatermark;
  private MistWatermarkEvent recentLeftWatermark;
  private MistWatermarkEvent recentRightWatermark;
  private long recentLeftTimestamp;
  private long recentRightTimestamp;

  public UnionOperator(final String operatorId) {
    super(operatorId);
    this.leftUpstreamQueue = new LinkedBlockingQueue<>();
    this.rightUpstreamQueue = new LinkedBlockingQueue<>();
    defaultWatermark = new MistWatermarkEvent(0L);
    this.recentLeftWatermark = defaultWatermark;
    this.recentRightWatermark = defaultWatermark;
    this.recentLeftTimestamp = 0L;
    this.recentRightTimestamp = 0L;
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
    while (!queue.isEmpty() && peekTimestamp(queue) <= timestamp) {
      final MistDataEvent event = queue.poll();
      outputEmitter.emitData(event);
    }
  }

  /**
   * Emits events which have less timestamp than the minimum watermark
   * calculated from the leftUpstreamQueue, currentLeftWatermark, rightUpstreamQueue, and currentRightWatermark,.
   */
  private void drainUntilMinimumWatermark() {
    final long leftWatermarkTimestamp = recentLeftWatermark.getTimestamp();
    final long rightWatermarkTimestamp = recentRightWatermark.getTimestamp();
    final long timestamp = getMinimumWatermark(leftWatermarkTimestamp, rightWatermarkTimestamp);
    LOG.log(Level.FINE, "{0} drains inputs until timestamp {1}",
        new Object[]{getOperatorIdentifier(), timestamp});

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
        final MistDataEvent event = leftUpstreamQueue.poll();
        outputEmitter.emitData(event);
      } else {
        final MistDataEvent event = rightUpstreamQueue.poll();
        outputEmitter.emitData(event);
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

    emitWatermarkUntilTimestamp(leftWatermarkTimestamp, rightWatermarkTimestamp, timestamp);
  }

  /**
   * Gets the minimum timestamp to drain.
   * We supposed that all MistDataEvents are arrived in-orderly,
   * but a data that has larger timestamp than a watermark could arrived faster than the watermark.
   * Therefore, we need to find the larger timestamp between the data and watermark in a single direction respectively,
   * and compare them to find minimum watermark.
   * @return the minimum timestamp to drain
   */
  private long getMinimumWatermark(final long leftWatermarkTimestamp, final long rightWatermarkTimestamp) {
    return Math.min(
        Math.max(recentLeftTimestamp, leftWatermarkTimestamp), Math.max(recentRightTimestamp, rightWatermarkTimestamp));
  }

  /**
   * Compares two recent watermarks with calculated minimum watermark and emits them.
   * @param leftWatermarkTimestamp the timestamp of current left watermark
   * @param rightWatermarkTimestamp the timestamp of current right watermark
   * @param minimumWatermark the calculated minimum watermark
   */
  private void emitWatermarkUntilTimestamp(final long leftWatermarkTimestamp,
                                           final long rightWatermarkTimestamp,
                                           final long minimumWatermark) {
    if (recentLeftWatermark != defaultWatermark && leftWatermarkTimestamp <= minimumWatermark) {
      outputEmitter.emitWatermark(recentLeftWatermark);
      recentLeftWatermark = defaultWatermark;
    }
    if (recentRightWatermark != defaultWatermark && rightWatermarkTimestamp <= minimumWatermark) {
      outputEmitter.emitWatermark(recentRightWatermark);
      recentRightWatermark = defaultWatermark;
    }
  }

  @Override
  public void processLeftData(final MistDataEvent event) {
    final long timestamp = event.getTimestamp();
    if (recentLeftTimestamp > timestamp) {
      throw new RuntimeException("The upstream events should be ordered by timestamp.");
    } else if (recentLeftWatermark.getTimestamp() > timestamp){
      throw new RuntimeException("The watermark should guarantee that " +
          "all events having less timestamp than it are arrived already.");
    }
    LOG.log(Level.FINE, "{0} gets left data {1}", new Object[]{getOperatorIdentifier(), event});
    recentLeftTimestamp = timestamp;
    leftUpstreamQueue.add(event);

    // Drain events until the minimum watermark.
    drainUntilMinimumWatermark();
  }

  @Override
  public void processRightData(final MistDataEvent event) {
    final long timestamp = event.getTimestamp();
    if (recentRightTimestamp > timestamp) {
      throw new RuntimeException("The upstream events should be ordered by timestamp.");
    } else if (recentRightWatermark.getTimestamp() > timestamp){
      throw new RuntimeException("The watermark should guarantee that " +
          "all events having less timestamp than it are arrived already.");
    }
    LOG.log(Level.FINE, "{0} gets right data {1}", new Object[]{getOperatorIdentifier(), event});
    recentRightTimestamp = timestamp;
    rightUpstreamQueue.add(event);

    // Drain events until the minimum watermark.
    drainUntilMinimumWatermark();
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent event) {
    if (event.getTimestamp() < recentLeftWatermark.getTimestamp()) {
      throw new RuntimeException("The timestamp of watermark should be greater or equal " +
          "than the previous event timestamp.");
    }
    LOG.log(Level.FINE, "{0} gets left watermark {1}", new Object[]{getOperatorIdentifier(), event});
    recentLeftWatermark = event;

    // Drain events until the minimum watermark.
    drainUntilMinimumWatermark();
  }

  @Override
  public void processRightWatermark(final MistWatermarkEvent event) {
    if (event.getTimestamp() < recentRightWatermark.getTimestamp()) {
      throw new RuntimeException("The timestamp of watermark should be greater or equal " +
          "than the previous event timestamp.");
    }
    LOG.log(Level.FINE, "{0} gets right watermark {1}", new Object[]{getOperatorIdentifier(), event});
    recentRightWatermark = event;

    // Drain events until the minimum watermark.
    drainUntilMinimumWatermark();
  }
}