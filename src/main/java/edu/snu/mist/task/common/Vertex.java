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
package edu.snu.mist.task.common;

import org.apache.avro.specific.SpecificRecord;
import org.apache.reef.wake.Identifier;

/**
 * Common interface for source, operator and sink.
 */
public interface Vertex {

  /**
   * Get the vertex identifier.
   * @return an identifier
   */
  Identifier getIdentifier();

  /**
   * Get the query identifier containing this vertex.
   * @return an identifier
   */
  Identifier getQueryIdentifier();

  /**
   * Get the attribute of the vertex.
   * @return attribute
   */
  SpecificRecord getAttribute();
}
