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

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.eventProcessors.EventProcessorManager;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This QueryManager is aware of the group and manages queries per group.
 * This has a global ThreadManager that manages event processors.
 * TODO[MIST-618]: Make GroupAwareGlobalSchedQueryManager use NextGroupSelector to schedule the group.
 */
@SuppressWarnings("unchecked")
final class GroupAwareGlobalSchedQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(GroupAwareGlobalSchedQueryManagerImpl.class.getName());

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
  private final GlobalSchedGroupInfoMap groupInfoMap;

  /**
   * Event processor manager that manages event processors globally.
   */
  private final EventProcessorManager eventProcessorManager;

  /**
   * A tracker measures global metrics such as total events number or cpu utilization.
   */
  private final GlobalSchedMetricTracker metricTracker;

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private GroupAwareGlobalSchedQueryManagerImpl(final DagGenerator dagGenerator,
                                                final ScheduledExecutorServiceWrapper schedulerWrapper,
                                                final GlobalSchedGroupInfoMap groupInfoMap,
                                                final EventProcessorManager eventProcessorManager,
                                                final QueryInfoStore planStore,
                                                final GlobalSchedMetricTracker metricTracker) {
    this.dagGenerator = dagGenerator;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.groupInfoMap = groupInfoMap;
    this.eventProcessorManager = eventProcessorManager;
    this.metricTracker = metricTracker;
    metricTracker.start();
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
        jcb.bindImplementation(QueryStarter.class, ImmediateQueryMergingStarter.class);
        final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
        groupInfoMap.putIfAbsent(groupId, injector.getInstance(GlobalSchedGroupInfo.class));
      }
      // Add the query into the group
      final GlobalSchedGroupInfo groupInfo = groupInfoMap.get(groupId);
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
    metricTracker.close();
    for (final GlobalSchedGroupInfo groupInfo : groupInfoMap.values()) {
      groupInfo.close();
    }
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
