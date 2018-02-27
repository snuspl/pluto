/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.core.task.groupaware;

import edu.snu.mist.core.task.ExecutionDags;
import edu.snu.mist.core.task.QueryRemover;
import edu.snu.mist.core.task.QueryStarter;
import edu.snu.mist.formats.avro.MetaGroupCheckpoint;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This interface represents a meta group that manages the split groups.
 * Network connections, codes are shared within this meta group.
 * Query merging is also being performed in this meta group.
 */
@DefaultImplementation(DefaultMetaGroupImpl.class)
public interface MetaGroup {

  /**
   * Get query starter.
   */
  QueryStarter getQueryStarter();

  /**
   * Get query remover.
   */
  QueryRemover getQueryRemover();

  /**
   * Get execution dags.
   * @return
   */
  ExecutionDags getExecutionDags();

  /**
   * Get split groups.
   */
  List<Group> getGroups();

  /**
   * Add a split group.
   * @param group split group
   */
  boolean addGroup(Group group);

  /**
   * The number of split groups.
   */
  AtomicInteger numGroups();

  /**
   * Set the jarFilePaths.
   * @param jarFilePaths
   */
  void setJarFilePaths(List<String> jarFilePaths);

  /**
   * Return a checkpoint of this MetaGroup.
   */
  MetaGroupCheckpoint checkpoint();
}