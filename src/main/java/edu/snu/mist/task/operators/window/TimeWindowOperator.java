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
package edu.snu.mist.task.operators.window;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.operators.BaseOperator;
import edu.snu.mist.task.operators.parameters.OperatorId;
import edu.snu.mist.task.operators.parameters.TimeWindowInterval;
import edu.snu.mist.task.operators.parameters.TimeWindowSize;
import edu.snu.mist.task.operators.parameters.TimeWindowStartTime;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class implements WindowOperator based on time.
 * The unit of window size and interval is in seconds.
 *
 * This class slices window size into buckets, which contains the list of inputs,
 * to generate windowed data every interval.
 *
 * The bucket size is determined by window size and interval.
 * In this implementation, the bucket size is calculated by (interval mod (window_size mod interval))
 *
 * At interval time, this operator collects the buckets until its window size is satisfied
 * and emits the collected data to output emitter.
 *
 * Ex) window size = 5 sec, interval = 2 sec
 * |----| <- one bucket (1 sec)
 * <---window size (5 sec)-->
 * |----|----|----|----|----|
 *           |----|----|----|----|----|
 *                     |----|----|----|----|----|
 * @param <I> input type
 */
public final class TimeWindowOperator<I>
    extends BaseOperator<I, WindowedData<I>> implements WindowOperator<I, Long> {

  /**
   * Buckets of window.
   */
  private final List<List<I>> buckets;

  /**
   * Size of bucket.
   */
  private final int bucketSize;

  /**
   * Maximum number of buckets.
   */
  private final int maxBuckets;

  /**
   * Time window interval (sec).
   */
  private final int timeWindowInterval;

  /**
   * Time window size (sec).
   */
  private final int timeWindowSize;

  /**
   * Start time of the windowing operator (nanoseconds).
   */
  private final long startTime;

  @Inject
  private TimeWindowOperator(@Parameter(OperatorId.class) final String operatorId,
                             @Parameter(QueryId.class) final String queryId,
                             @Parameter(TimeWindowInterval.class) final int timeWindowInterval,
                             @Parameter(TimeWindowSize.class) final int timeWindowSize,
                             @Parameter(TimeWindowStartTime.class) final long startTime,
                             final StringIdentifierFactory identifierFactory) {
    super(identifierFactory.getNewInstance(queryId), identifierFactory.getNewInstance(operatorId));
    this.buckets = new LinkedList<>();
    this.buckets.add(new LinkedList<>());
    this.timeWindowSize = timeWindowSize;
    this.timeWindowInterval = timeWindowInterval;
    this.bucketSize = calculateBucketSize();
    this.maxBuckets = timeWindowSize / bucketSize;
    this.startTime = startTime;
  }

  /**
   * Calculate bucket size.
   * @return bucket size
   */
  private int calculateBucketSize() {
    if (timeWindowSize % timeWindowInterval == 0) {
      return timeWindowInterval;
    } else {
      return timeWindowInterval % (timeWindowSize % timeWindowInterval);
    }
  }
  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.TIME_WINDOW;
  }

  @Override
  public void handle(final I input) {
    synchronized (buckets) {
      final List<I> bucket = buckets.get(buckets.size() - 1);
      bucket.add(input);
    }
  }

  /**
   * Receive notification and emits or slides buckets.
   * @param notificationTime tick time (nanoseconds)
   */
  @Override
  public void windowNotification(final Long notificationTime) {
    final long elapsedTime = TimeUnit.NANOSECONDS.toSeconds(notificationTime - startTime);
    // Time to emit windowed data
    if (elapsedTime % timeWindowInterval == 0) {
      // output emit
      synchronized (buckets) {
        final List<I> windowedData = new LinkedList<>();
        for (final List<I> bucket : buckets) {
          windowedData.addAll(bucket);
        }
        outputEmitter.emit(new WindowedData<>(windowedData));
      }
    }
    // Slide buckets
    if (elapsedTime % bucketSize == 0) {
      synchronized (buckets) {
        if (buckets.size() == maxBuckets) {
          buckets.remove(0);
        }
        buckets.add(new LinkedList<>());
      }
    }
  }
}
