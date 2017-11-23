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
public final class DefaultPhysicalOperatorImpl extends BasePhysicalVertex implements PhysicalOperator {

  /**
   * The operator that processes events.
   */
  private final Operator operator;

  /**
   * The timestamp of the data that is recently processed.
   */
  private long latestDataTimestamp;

  /**
   * The timestamp of the watermark that is recently processed.
   */
  private long latestWatermarkTimestamp;

  public DefaultPhysicalOperatorImpl(final String id,
                                     final String configuration,
                                     final Operator operator) {
    super(id, configuration);
    this.operator = operator;
  }

  @Override
  public Operator getOperator() {
    return operator;
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

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DefaultPhysicalOperatorImpl that = (DefaultPhysicalOperatorImpl) o;

    if (!id.equals(that.id)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public Type getType() {
    return Type.OPERATOR;
  }

  @Override
  public String getIdentifier() {
    return id;
  }
}
