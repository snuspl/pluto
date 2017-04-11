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

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of QueryManager.
 */
@SuppressWarnings("unchecked")
final class DefaultQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(DefaultQueryManagerImpl.class.getName());

  /**
   * Map of query id and logical dag.
   */
  private final ConcurrentMap<String, DAG<LogicalVertex, MISTEdge>> logicalDagMap;

  /**
   * A thread manager.
   */
  private final ThreadManager threadManager;

  /**
   * Scheduler for periodic watermark emission.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * A plan store.
   */
  private final QueryInfoStore planStore;

  /**
   * A execution and logical dag generator.
   */
  private final DagGenerator dagGenerator;

  /**
   * A query starter.
   */
  private final QueryStarter queryStarter;

  /**
   * A map which contains groups and their information.
   */
  private final ConcurrentMap<String, GroupInfo> groupInfoMap;

  /**
   * Default query manager in MistTask.
   * @param dagGenerator the generator that generates the logical and execution dag from avro operator chain dag.
   * @param threadManager thread manager
   */
  @Inject
  private DefaultQueryManagerImpl(final DagGenerator dagGenerator,
                                  final ThreadManager threadManager,
                                  final ScheduledExecutorServiceWrapper schedulerWrapper,
                                  final QueryStarter queryStarter,
                                  final QueryInfoStore planStore) {
    this.logicalDagMap = new ConcurrentHashMap<>();
    this.dagGenerator = dagGenerator;
    this.threadManager = threadManager;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.queryStarter = queryStarter;
    this.groupInfoMap = new ConcurrentHashMap<>();
  }

  /**
   * It converts the avro operator chain dag (query) to the execution dag,
   * and executes the sources in order to receives data streams.
   * Before the queries are executed, it stores the avro operator chain dag into disk.
   * We can regenerate the queries from the stored avro operator chain dag.
   * @param tuple a pair of the query id and the avro operator chain dag
   * @return submission result
   */
  @Override
  public QueryControlResult create(final Tuple<String, AvroOperatorChainDag> tuple) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(tuple.getKey());
    try {
      // 1) Saves the avro operator chain dag to the PlanStore and
      // converts the avro operator chain dag to the logical and execution dag
      planStore.saveAvroOpChainDag(tuple);
      final LogicalAndExecutionDag logicalAndExecutionDag = dagGenerator.generate(tuple);
      final String queryId = tuple.getKey();
      // Store the logical dag in memory
      logicalDagMap.putIfAbsent(queryId, logicalAndExecutionDag.getLogicalDag());
      // Update group information
      final String groupId = tuple.getValue().getGroupId();
      if (!groupInfoMap.containsKey(groupId)) {
        // Add new group id, if it doesn't exist
        groupInfoMap.putIfAbsent(groupId, Tang.Factory.getTang().newInjector().getInstance(GroupInfo.class));
      }
      // Add the query into the group
      groupInfoMap.get(groupId).addQueryIdToGroup(queryId);
      // Start the submitted dag
      queryStarter.start(logicalAndExecutionDag.getExecutionDag());
      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(tuple.getKey()));
      return queryControlResult;
    } catch (final Exception e) {
      // [MIST-345] We need to release all of the information that is required for the query when it fails.
      LOG.log(Level.SEVERE, "An exception occurred while starting {0} query: {1}",
          new Object[] {tuple.getKey(), e.toString()});
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(e.getMessage());
      return queryControlResult;
    }
  }

  @Override
  public void close() throws Exception {
    scheduler.shutdown();
    threadManager.close();
  }

  /**
   * Deletes queries from MIST.
   * TODO[MIST-431] To delete the queries, we need to implement reference counting mechanism.
   */
  @Override
  public QueryControlResult delete(final String queryId) {
    throw new RuntimeException("Deleting queries is not implemented yet. We need to address [MIST-431]");
  }
}
