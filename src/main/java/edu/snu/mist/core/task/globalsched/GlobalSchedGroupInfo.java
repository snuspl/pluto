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
package edu.snu.mist.core.task.globalsched;

import edu.snu.mist.core.task.ExecutionDags;
import edu.snu.mist.core.task.OperatorChainManager;
import edu.snu.mist.core.task.QueryRemover;
import edu.snu.mist.core.task.QueryStarter;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * A class which contains query and metric information about query group.
 * It is different from GroupInfo in that it does not have ThreadManager.
 * As we consider global scheduling, we do not have to have ThreadManger per group.
 */
@DefaultImplementation(DefaultGlobalSchedGroupInfo.class)
interface GlobalSchedGroupInfo extends AutoCloseable {

  /**
   * Get the operator chain manager.
   * @return operator chain manager
   */
  OperatorChainManager getOperatorChainManager();


  /**
   * Add query id to the group.
   * @param queryId query id
   */
  void addQueryIdToGroup(String queryId);

  /**
   * Get the query starter.
   * @return query starter
   */
  QueryStarter getQueryStarter();

  /**
   * @return the list of query id in this group
   */
  List<String> getQueryIdList();

  /**
   * Return the execution dags in the group.
   * @return execution dags
   */
  ExecutionDags<String> getExecutionDags();

  /**
   * Return the query remover.
   * @return query remover
   */
  QueryRemover getQueryRemover();
}