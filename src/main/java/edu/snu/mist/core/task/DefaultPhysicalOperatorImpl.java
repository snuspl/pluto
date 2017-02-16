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

import edu.snu.mist.common.operators.Operator;

/**
 * This is the default implementation of PhysicalOperator.
 */
final class DefaultPhysicalOperatorImpl extends BasePhysicalVertex implements PhysicalOperator {

  /**
   * The operator that processes events.
   */
  private final Operator operator;

  /**
   * The partitioned query that holds the operator.
   */
  private PartitionedQuery partitionedQuery;

  /**
   * The timestamp of the data that is recently processed.
   */
  private long latestDataTimestamp;

  /**
   * The timestamp of the watermark that is recently processed.
   */
  private long latestWatermarkTimestamp;

  public DefaultPhysicalOperatorImpl(final Operator operator,
                                     final PartitionedQuery partitionedQuery) {
    this(operator, partitionedQuery, null);
  }

  public DefaultPhysicalOperatorImpl(final Operator operator,
                                     final PartitionedQuery partitionedQuery,
                                     final String configuration) {
    super(configuration);
    this.operator = operator;
    this.partitionedQuery = partitionedQuery;
  }

  @Override
  public Operator getOperator() {
    return operator;
  }

  @Override
  public PartitionedQuery getPartitionedQuery() {
    return partitionedQuery;
  }

  @Override
  public void setPartitionedQuery(final PartitionedQuery newPartitionedQuery) {
    partitionedQuery = newPartitionedQuery;
  }

  @Override
  public Type getType() {
    return Type.OPERATOR;
  }

  @Override
  public long getLatestDataTimestamp() {
    return latestDataTimestamp;
  }

  @Override
  public void setLatestDataTimestamp(final long timestamp) {
    latestDataTimestamp = timestamp;
  }

  @Override
  public long getLatestWatermarkTimestamp() {
    return latestWatermarkTimestamp;
  }

  @Override
  public void setLatestWatermarkTimestamp(final long timestamp) {
    latestWatermarkTimestamp = timestamp;
  }
}
