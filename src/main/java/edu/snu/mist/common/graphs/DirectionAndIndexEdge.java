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
package edu.snu.mist.common.graphs;

import edu.snu.mist.formats.avro.Direction;

/**
 * This implements an edge that has direction and index information.
 */
public final class DirectionAndIndexEdge extends DirectionEdge implements EdgeInfo {

  /**
   * A index information.
   */
  private final Integer index;

  public DirectionAndIndexEdge(final Direction direction,
                               final Integer index) {
    super(direction);
    this.index = index;
  }

  /**
   * @return the index information
   */
  public Integer getIndex() {
    return index;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof DirectionAndIndexEdge)) {
      return false;
    } else if (!super.equals(o)) {
      return false;
    } else {
      return index == ((DirectionAndIndexEdge) o).getIndex();
    }
  }

  @Override
  public int hashCode() {
    return super.hashCode() + 10 * index.hashCode();
  }
}
