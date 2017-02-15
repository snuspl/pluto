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
 * This class represents an edge that has direction and index information.
 */
public final class MISTEdge {

  /**
   * A index information.
   */
  private final Integer index;
  /**
   * A direction information.
   */
  private final Direction direction;
  /**
   * The default value of index.
   */
  private static final Integer DEFAULT_INDEX = 0;

  public MISTEdge(final Direction direction) {
    this(direction, DEFAULT_INDEX);
  }

  public MISTEdge(final Direction direction,
                  final Integer index) {
    this.direction = direction;
    this.index = index;
  }

  /**
   * @return the index information
   */
  public Integer getIndex() {
    return index;
  }

  /**
   * @return the direction information
   */
  public Direction getDirection() {
    return direction;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof MISTEdge)) {
      return false;
    } else {
      return direction == ((MISTEdge) o).getDirection() &&
          index == ((MISTEdge) o).getIndex();
    }
  }

  @Override
  public int hashCode() {
    return 10 * direction.hashCode() + index.hashCode();
  }
}
