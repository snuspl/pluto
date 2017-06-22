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
import edu.snu.mist.common.shared.KafkaSharedResource;
import edu.snu.mist.common.shared.MQTTSharedResource;
import edu.snu.mist.common.shared.NettySharedResource;
import edu.snu.mist.core.driver.parameters.DeactivationEnabled;
import edu.snu.mist.core.driver.parameters.GroupAware;
import edu.snu.mist.core.driver.parameters.MergingEnabled;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.batchsub.BatchQueryCreator;
import edu.snu.mist.core.task.deactivation.DeactivationGroupSourceManager;
import edu.snu.mist.core.task.deactivation.GroupSourceManager;
import edu.snu.mist.core.task.deactivation.NoDeactivationGroupSourceManager;
import edu.snu.mist.core.task.eventProcessors.EventProcessorManager;
import edu.snu.mist.core.task.globalsched.metrics.CpuUtilMetricEventHandler;
import edu.snu.mist.core.task.globalsched.metrics.EventNumAndWeightMetricEventHandler;
import edu.snu.mist.core.task.globalsched.metrics.NumGroupsMetricEventHandler;
import edu.snu.mist.core.task.globalsched.parameters.GroupSchedModelType;
import edu.snu.mist.core.task.merging.ImmediateQueryMergingStarter;
import edu.snu.mist.core.task.merging.MergeAwareQueryRemover;
import edu.snu.mist.core.task.merging.MergingExecutionDags;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This QueryManager is aware of the group and manages queries per group.
 * This has a global ThreadManager that manages event processors.
 * TODO[MIST-618]: Make GroupAwareGlobalSchedQueryManager use NextGroupSelector to schedule the group.
 */
@SuppressWarnings("unchecked")
public final class GroupAwareGlobalSchedQueryManagerImpl implements QueryManager {

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
   * A map which contains groups and their information.
   */
  private final GlobalSchedGroupInfoMap groupInfoMap;

  /**
   * A tracker measures global metrics such as total events number or cpu utilization.
   */
  private final MetricTracker metricTracker;

  /**
   * Merging enabled or not.
   */
  private final boolean mergingEnabled;

  /**
   * Deactivation enabled or not.
   */
  private final boolean deactivationEnabled;

  /**
   * Event processor manager.
   */
  private final EventProcessorManager eventProcessorManager;

  /**
   * A batch query submission helper.
   */
  private final BatchQueryCreator batchQueryCreator;

  /**
   * A dag generator that creates DAG<ConfigVertex, MISTEdge> from avro vertex chain dag.
   */
  private final ConfigDagGenerator configDagGenerator;

  /**
   * The execution model of event processor (dispatching).
   */
  private final String executionModel;

  /**
   * A globally shared MQTTSharedResource.
   */
  private final MQTTSharedResource mqttSharedResource;

  /**
   * A globally shared KafkaSharedResource.
   */
  private final KafkaSharedResource kafkaSharedResource;

  /**
   * A globally shared NettySharedResource.
   */
  private final NettySharedResource nettySharedResource;

  /**
   * TODO[REMOVE]: false if group unaware execution model.
   */
  private final boolean groupAware;

  // TODO[REMOVE]
  private final AtomicLong groupIdCounter = new AtomicLong(0);
  private final DagGenerator dagGenerator;

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private GroupAwareGlobalSchedQueryManagerImpl(final ScheduledExecutorServiceWrapper schedulerWrapper,
                                                final GlobalSchedGroupInfoMap groupInfoMap,
                                                final QueryInfoStore planStore,
                                                final MistPubSubEventHandler pubSubEventHandler,
                                                final EventProcessorManager eventProcessorManager,
                                                @Parameter(MergingEnabled.class) final boolean mergingEnabled,
                                                @Parameter(DeactivationEnabled.class) final boolean deactivateEnabled,
                                                final MetricTracker metricTracker,
                                                final ConfigDagGenerator configDagGenerator,
                                                final EventNumAndWeightMetricEventHandler eventNumHandler,
                                                final CpuUtilMetricEventHandler cpuUtilHandler,
                                                final NumGroupsMetricEventHandler numGroupsHandler,
                                                final MemoryUsageMetricEventHandler memUsageHandler,
                                                final EventProcessorNumAssigner assigner,
                                                final BatchQueryCreator batchQueryCreator,
                                                final MQTTSharedResource mqttSharedResource,
                                                final KafkaSharedResource kafkaSharedResource,
                                                final NettySharedResource nettySharedResource,
                                                final DagGenerator dagGenerator,
                                                @Parameter(GroupAware.class) final boolean groupAware,
                                                @Parameter(GroupSchedModelType.class) final String executionModel) {
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.groupInfoMap = groupInfoMap;
    this.metricTracker = metricTracker;
    this.mergingEnabled = mergingEnabled;
    this.deactivationEnabled = deactivateEnabled;
    this.eventProcessorManager = eventProcessorManager;
    this.configDagGenerator = configDagGenerator;
    this.batchQueryCreator = batchQueryCreator;
    this.executionModel = executionModel;
    this.mqttSharedResource = mqttSharedResource;
    this.kafkaSharedResource = kafkaSharedResource;
    this.nettySharedResource = nettySharedResource;
    this.dagGenerator = dagGenerator;
    this.groupAware = groupAware;
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
      final String groupId;
      if (groupAware) {
        groupId = tuple.getValue().getGroupId();
      } else {
        // TODO[REMOVE]: If it is groupUnaware, just assign unique group Id per query
        groupId = Long.toString(groupIdCounter.getAndIncrement());
      }

      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "Create Query [gid: {0}, qid: {1}]",
            new Object[]{groupId, queryId});
      }

      if (groupInfoMap.get(groupId) == null) {
        // Add new group id, if it doesn't exist
        final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
        jcb.bindNamedParameter(GroupId.class, groupId);

        // TODO[DELETE] start: for test
        if (mergingEnabled) {
          jcb.bindImplementation(QueryStarter.class, ImmediateQueryMergingStarter.class);
          jcb.bindImplementation(QueryRemover.class, MergeAwareQueryRemover.class);
          jcb.bindImplementation(ExecutionDags.class, MergingExecutionDags.class);
        } else {
          jcb.bindImplementation(QueryStarter.class, NoMergingQueryStarter.class);
          jcb.bindImplementation(QueryRemover.class, NoMergingAwareQueryRemover.class);
          jcb.bindImplementation(ExecutionDags.class, NoMergingExecutionDags.class);
        }

        if (deactivationEnabled) {
          jcb.bindImplementation(GroupSourceManager.class, DeactivationGroupSourceManager.class);
        } else {
          jcb.bindImplementation(GroupSourceManager.class, NoDeactivationGroupSourceManager.class);
        }

        switch (executionModel) {
          case "dispatching":
            jcb.bindImplementation(OperatorChainManager.class, NonBlockingActiveOperatorChainPickManager.class);
            break;
          default:
            throw new RuntimeException("Invalid execution model: " + executionModel);
        }
        // TODO[DELETE] end: for test

        final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
        injector.bindVolatileInstance(MQTTSharedResource.class, mqttSharedResource);
        injector.bindVolatileInstance(KafkaSharedResource.class, kafkaSharedResource);
        injector.bindVolatileInstance(NettySharedResource.class, nettySharedResource);
        injector.bindVolatileInstance(QueryInfoStore.class, planStore);
        injector.bindVolatileInstance(DagGenerator.class, dagGenerator);
        final GlobalSchedGroupInfo groupInfo = injector.getInstance(GlobalSchedGroupInfo.class);
        if (groupInfoMap.putIfAbsent(groupId, groupInfo) == null) {

          if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "Create Group: {0}", new Object[]{groupId});
          }

          eventProcessorManager.addGroup(groupInfo);
        }
      }
      // Add the query into the group
      final GlobalSchedGroupInfo groupInfo = groupInfoMap.get(groupId);
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
    metricTracker.close();
    for (final GlobalSchedGroupInfo groupInfo : groupInfoMap.values()) {
      groupInfo.close();
    }
    eventProcessorManager.close();
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
