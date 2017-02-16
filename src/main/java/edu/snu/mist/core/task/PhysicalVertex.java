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

/**
 * This interface represents physical vertices of the query.
 */
interface PhysicalVertex {

  public static enum Type {
    SOURCE,
    OPERATOR_CHIAN,
    OPERATOR,
    SINK
  }

  /**
   * Get the type of the physical vertex.
   */
  Type getType();

  /**
   * Get the configuration of the physical vertex.
   * This will be used to merge two vertices.
   * @return the serialized configuration
   */
  String getConfiguration();
}
