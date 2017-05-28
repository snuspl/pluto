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
import edu.snu.mist.core.driver.parameters.MergingEnabled;
import edu.snu.mist.core.task.deactivation.GroupSourceManager;
import edu.snu.mist.core.task.batchsub.BatchQueryCreator;
import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.merging.ImmediateQueryMergingStarter;
import edu.snu.mist.core.task.merging.MergeAwareQueryRemover;
import edu.snu.mist.core.task.merging.MergingExecutionDags;
import edu.snu.mist.core.task.metrics.EventNumMetricEventHandler;
import edu.snu.mist.core.task.metrics.EventProcessorNumAssigner;
import edu.snu.mist.core.task.metrics.MemoryUsageMetricEventHandler;
import edu.snu.mist.core.task.metrics.MetricTracker;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This QueryManager is unaware of the group. This just schedules queries.
 */
@SuppressWarnings("unchecked")
public final class GroupUnawareQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(GroupUnawareQueryManagerImpl.class.getName());

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
   * A tracker that measures the metric.
   */
  private final MetricTracker metricTracker;

  /**
   * A event processor number assigner.
   */
  private final EventProcessorNumAssigner assigner;

  /**
   * Enabled the query merging or not.
   */
  private final boolean mergingEnabled;

  /**
   * A batch query submission helper.
   */
  private final BatchQueryCreator batchQueryCreator;

  /**
   * A dag generator that creates DAG<ConfigVertex, MISTEdge> from avro vertex chain dag.
   */
  private final ConfigDagGenerator configDagGenerator;

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private GroupUnawareQueryManagerImpl(final DagGenerator dagGenerator,
                                       final ScheduledExecutorServiceWrapper schedulerWrapper,
                                       final GroupInfoMap groupInfoMap,
                                       @Parameter(DefaultNumEventProcessors.class) final int numEventProcessors,
                                       final QueryInfoStore planStore,
                                       @Parameter(MergingEnabled.class) final boolean mergingEnabled,
                                       final MetricTracker metricTracker,
                                       final ConfigDagGenerator configDagGenerator,
                                       final EventProcessorNumAssigner assigner,
                                       final EventNumMetricEventHandler eventNumHandler,
                                       final MemoryUsageMetricEventHandler memoryUsageHandler,
                                       final BatchQueryCreator batchQueryCreator) {
    this.dagGenerator = dagGenerator;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.groupInfoMap = groupInfoMap;
    this.numEventProcessors = numEventProcessors;
    this.mergingEnabled = mergingEnabled;
    this.metricTracker = metricTracker;
    this.assigner = assigner;
    this.configDagGenerator = configDagGenerator;
    this.batchQueryCreator = batchQueryCreator;
    metricTracker.start();
  }

  /**
   * Start a submitted query.
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
      // Create the submitted query
      // 1) Saves the avro operator chain dag to the PlanStore and
      // converts the avro operator chain dag to the logical and execution dag
      planStore.saveAvroOpChainDag(tuple);

      final String queryId = tuple.getKey();
      // Update group information
      //final String groupId = tuple.getValue().getGroupId();
      final String groupId = "1";
      if (groupInfoMap.get(groupId) == null) {
        // Add new group id, if it doesn't exist
        final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
        jcb.bindNamedParameter(GroupId.class, groupId);
        jcb.bindNamedParameter(DefaultNumEventProcessors.class, Integer.toString(numEventProcessors));
        if (mergingEnabled) {
          jcb.bindImplementation(QueryStarter.class, ImmediateQueryMergingStarter.class);
          jcb.bindImplementation(QueryRemover.class, MergeAwareQueryRemover.class);
          jcb.bindImplementation(ExecutionDags.class, MergingExecutionDags.class);
        } else {
          jcb.bindImplementation(QueryStarter.class, NoMergingQueryStarter.class);
          jcb.bindImplementation(QueryRemover.class, NoMergingAwareQueryRemover.class);
          jcb.bindImplementation(ExecutionDags.class, NoMergingExecutionDags.class);
        }
        final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
        injector.bindVolatileInstance(DagGenerator.class, dagGenerator);
        injector.bindVolatileInstance(QueryInfoStore.class, planStore);
        groupInfoMap.putIfAbsent(groupId, injector.getInstance(GroupInfo.class));
      }
      // Add the query into the group
      final GroupInfo groupInfo = groupInfoMap.get(groupId);
      groupInfo.addQueryIdToGroup(queryId);

      // Start the submitted dag
      final DAG<ConfigVertex, MISTEdge> configDag = configDagGenerator.generate(tuple.getValue());
      groupInfo.getQueryStarter().start(queryId, configDag, tuple.getValue().getJarFilePaths());

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

  /**
   * TODO[DELETE] this code is for test.
   * Start submitted queries in batch manner.
   * The operator chain dag will be duplicated for test.
   * @param tuple a pair of the query id and the avro operator chain dag
   * @return submission result
   */
  @Override
  public QueryControlResult createBatch(final Tuple<List<String>, AvroOperatorChainDag> tuple) {
    final List<String> queryIdList = tuple.getKey();
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryIdList.get(0));
    try {
      batchQueryCreator.duplicate(tuple, this);

      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(tuple.getKey().get(0)));
      return queryControlResult;
    } catch (final Exception e) {
      e.printStackTrace();
      // [MIST-345] We need to release all of the information that is required for the query when it fails.
      LOG.log(Level.SEVERE, "An exception occurred while starting from {0} to {1} batch query: {2}",
          new Object[] {queryIdList.get(0), queryIdList.get(queryIdList.size() - 1), e.toString()});
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(e.getMessage());
      return queryControlResult;
    }
  }

  @Override
  public void close() throws Exception {
    scheduler.shutdown();
    planStore.close();
    for (final GroupInfo groupInfo : groupInfoMap.values()) {
      groupInfo.close();
    }
    metricTracker.close();
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

  @Override
  public GroupSourceManager getGroupSourceManager(final String groupId) {
    return groupInfoMap.get(groupId).getGroupSourceManager();
  }
}
