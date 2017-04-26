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
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.queryStarters.ImmediateQueryMergingStarter;
import edu.snu.mist.core.task.queryStarters.QueryStarter;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This QueryManager is aware of the group and manages queries per group.
 */
@SuppressWarnings("unchecked")
final class GroupAwareQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(GroupAwareQueryManagerImpl.class.getName());

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
   * A map which contains groups and their information.
   */
  private final GroupInfoMap groupInfoMap;

  /**
   * The number of event processors per group.
   */
  private final int numEventProcessors;

  /**
   * A tracker that measures the metric of each group.
   */
  private final GroupMetricTracker groupTracker;

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private GroupAwareQueryManagerImpl(final DagGenerator dagGenerator,
                                     final ScheduledExecutorServiceWrapper schedulerWrapper,
                                     final GroupInfoMap groupInfoMap,
                                     @Parameter(DefaultNumEventProcessors.class) final int numEventProcessors,
                                     final QueryInfoStore planStore,
                                     final GroupMetricTracker groupTracker) {
    this.dagGenerator = dagGenerator;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.groupInfoMap = groupInfoMap;
    this.numEventProcessors = numEventProcessors;
    this.groupTracker = groupTracker;
    groupTracker.start();
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
      final DAG<ExecutionVertex, MISTEdge> executionDag = dagGenerator.generate(tuple);
      final String queryId = tuple.getKey();
      // Update group information
      final String groupId = tuple.getValue().getGroupId();
      if (groupInfoMap.get(groupId) == null) {
        // Add new group id, if it doesn't exist
        final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
        jcb.bindNamedParameter(GroupId.class, groupId);
        jcb.bindNamedParameter(DefaultNumEventProcessors.class, Integer.toString(numEventProcessors));
        jcb.bindImplementation(QueryStarter.class, ImmediateQueryMergingStarter.class);
        final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
        groupInfoMap.putIfAbsent(groupId, injector.getInstance(GroupInfo.class));
      }
      // Add the query into the group
      final GroupInfo groupInfo = groupInfoMap.get(groupId);
      groupInfo.addQueryIdToGroup(queryId);
      // Start the submitted dag
      groupInfo.getQueryStarter().start(queryId, executionDag);
      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(tuple.getKey()));
      return queryControlResult;
    } catch (final Exception e) {
      e.printStackTrace();
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
    for (final GroupInfo groupInfo : groupInfoMap.values()) {
      groupInfo.close();
    }
    groupTracker.close();
  }

  /**
   * Deletes queries from MIST.
   */
  @Override
  public QueryControlResult delete(final String groupId, final String queryId) {
    groupInfoMap.get(groupId).getQueryRemover().deleteQuery(queryId);
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryId);
    queryControlResult.setIsSuccess(true);
    queryControlResult.setMsg(ResultMessage.deleteSuccess(queryId));
    return queryControlResult;
  }
}
