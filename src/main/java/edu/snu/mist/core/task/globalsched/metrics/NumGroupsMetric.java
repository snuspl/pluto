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
package edu.snu.mist.core.task.globalsched.metrics;

import javax.inject.Inject;

/**
 * A class which contains metric of the number of groups.
 */
public final class NumGroupsMetric {

  /**
   * The number of groups.
   */
  private volatile long numGroups;

  @Inject
  private NumGroupsMetric() {
    this.numGroups = 0;
  }

  public long getNumGroups() {
    return numGroups;
  }

  public void setNumGroups(final long numGroups) {
    this.numGroups = numGroups;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof NumGroupsMetric)) {
      return false;
    }
    final NumGroupsMetric numGroupsMetric = (NumGroupsMetric) o;
    return this.numGroups == numGroupsMetric.getNumGroups();
  }

  @Override
  public int hashCode() {
    return ((Long) this.numGroups).hashCode() * 33;
  }
}