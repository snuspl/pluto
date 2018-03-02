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
import edu.snu.mist.formats.avro.ApplicationInfoCheckpoint;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This interface represents an application.
 * Network connections, codes are shared within this application.
 * Query merging is also being performed in the same application.
 */
@DefaultImplementation(DefaultApplicationInfoImpl.class)
public interface ApplicationInfo {

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
   * Get groups of the application.
   */
  List<Group> getGroups();

  /**
   * Add a group.
   * @param group group
   */
  boolean addGroup(Group group);

  /**
   * The number of groups.
   */
  AtomicInteger numGroups();

  /**
   * Get the application id.
   * @return
   */
  String getApplicationId();

  /**
   * Get a jar file path of the application.
   * @return
   */
  List<String> getJarFilePath();

  /**
   * Return a checkpoint of this app.
   */
  ApplicationInfoCheckpoint checkpoint();
}