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

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This interface holds a map of <PhysicalVertexId, PhysicalVertex>.
 * With this map, we can decouple the logical plan of the query and its physical plan.
 * This map will be used to delete queries.
 */
final class DefaultPhysicalVertexMapImpl implements PhysicalVertexMap {

  private final ConcurrentMap<String, PhysicalVertex> physicalVertexMap;

  @Inject
  private DefaultPhysicalVertexMapImpl() {
    this.physicalVertexMap = new ConcurrentHashMap<>();
  }

  public ConcurrentMap<String, PhysicalVertex> getPhysicalVertexMap() {
    return physicalVertexMap;
  }
}
