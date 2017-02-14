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
 * This implements an edge that only has direction information.
 */
public class DirectionEdge implements EdgeInfo {

  /**
   * A direction information.
   */
  private final Direction direction;

  public DirectionEdge(final Direction direction) {
    this.direction = direction;
  }

  /**
   * @return the direction information
   */
  public Direction getDirection() {
    return direction;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof DirectionEdge)) {
      return false;
    } else {
      return direction == ((DirectionEdge) o).getDirection();
    }
  }

  @Override
  public int hashCode() {
    return 10 * direction.hashCode();
  }
}
