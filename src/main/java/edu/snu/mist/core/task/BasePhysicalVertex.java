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

import java.util.HashSet;
import java.util.Set;

/**
 * This interface represents physical vertices of the query.
 * The physical vertex is one of the source, operator, or sink.
 * It holds the meta data of the source, operator, or sink.
 */
abstract class BasePhysicalVertex implements PhysicalVertex {

  /**
   * Get the id of the vertex.
   */
  protected final String id;

  /**
   * Get the configuration.
   */
  protected final String configuration;

  /**
   * The set of dependent and active sources.
   */
  protected Set<String> activeSourceIdSet;

  public BasePhysicalVertex(final String id,
                            final String configuration) {
    this.id = id;
    this.configuration = configuration;
    this.activeSourceIdSet = new HashSet<>();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getConfiguration() {
    return configuration;
  }
}
