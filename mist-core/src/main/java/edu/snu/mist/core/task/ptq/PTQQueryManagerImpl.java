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
package edu.snu.mist.core.task.ptq;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.common.shared.KafkaSharedResource;
import edu.snu.mist.common.shared.MQTTResource;
import edu.snu.mist.common.shared.NettySharedResource;
import edu.snu.mist.core.driver.parameters.DeactivationEnabled;
import edu.snu.mist.core.driver.parameters.GroupAware;
import edu.snu.mist.core.driver.parameters.MergingEnabled;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.batchsub.BatchQueryCreator;
import edu.snu.mist.core.task.deactivation.GroupSourceManager;
import edu.snu.mist.core.task.groupaware.*;
import edu.snu.mist.core.task.groupaware.parameters.GroupSchedModelType;
import edu.snu.mist.core.task.merging.*;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This query manager represents stream database model
 * that shares processes, threads, and performs query merging.
 * We can share network connections and query codes on top of this execution model by configuring parameters.
 */
@SuppressWarnings("unchecked")
public final class PTQQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(PTQQueryManagerImpl.class.getName());

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
   * A map that has a group identifier as a key
   * and a meta group (for sharing code and network connections) as a value.
   */
  private final ConcurrentMap<String, Tuple<MetaGroup, AtomicBoolean>> groupMap;

  /**
   * Merging enabled or not.
   */
  private final boolean mergingEnabled;

  /**
   * Event processor manager.
   */
  private final EventProcessorManager eventProcessorManager;

  /**
   * A batch query submission helper.
   */
  private final BatchQueryCreator batchQueryCreator;

  /**
   * A dag generator that creates DAG<ConfigVertex, MISTEdge> from avro dag.
   */
  private final ConfigDagGenerator configDagGenerator;

  /**
   * A globally shared MQTTSharedResource.
   */
  private final MQTTResource mqttSharedResource;

  /**
   * A globally shared KafkaSharedResource.
   */
  private final KafkaSharedResource kafkaSharedResource;

  /**
   * A globally shared NettySharedResource.
   */
  private final NettySharedResource nettySharedResource;

  private final DagGenerator dagGenerator;

  private final GroupAllocationTableModifier groupAllocationTableModifier;

  private final AtomicLong groupIdCounter = new AtomicLong(0);

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private PTQQueryManagerImpl(final ScheduledExecutorServiceWrapper schedulerWrapper,
                              final GlobalSchedGroupInfoMap groupInfoMap,
                              final QueryInfoStore planStore,
                              final EventProcessorManager eventProcessorManager,
                              @Parameter(MergingEnabled.class) final boolean mergingEnabled,
                              @Parameter(DeactivationEnabled.class) final boolean deactivateEnabled,
                              final ConfigDagGenerator configDagGenerator,
                              final BatchQueryCreator batchQueryCreator,
                              final MQTTResource mqttSharedResource,
                              final KafkaSharedResource kafkaSharedResource,
                              final NettySharedResource nettySharedResource,
                              final DagGenerator dagGenerator,
                              @Parameter(GroupAware.class) final boolean groupAware,
                              final GroupAllocationTableModifier groupAllocationTableModifier,
                              @Parameter(GroupSchedModelType.class) final String executionModel) {
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.groupInfoMap = groupInfoMap;
    this.mergingEnabled = mergingEnabled;
    this.eventProcessorManager = eventProcessorManager;
    this.configDagGenerator = configDagGenerator;
    this.batchQueryCreator = batchQueryCreator;
    this.mqttSharedResource = mqttSharedResource;
    this.kafkaSharedResource = kafkaSharedResource;
    this.nettySharedResource = nettySharedResource;
    this.dagGenerator = dagGenerator;
    this.groupAllocationTableModifier = groupAllocationTableModifier;
    this.groupMap = new ConcurrentHashMap<>();
  }

  /**
   * Start a submitted query.
   * It converts the avro operator chain dag (query) to the execution dag,
   * and executes the sources in order to receives data streams.
   * Before the queries are executed, it stores the avro  dag into disk.
   * We can regenerate the queries from the stored avro dag.
   * @param tuple a pair of the query id and the avro dag
   * @return submission result
   */
  @Override
  public QueryControlResult create(final Tuple<String, AvroDag> tuple) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(tuple.getKey());
    try {
      // Create the submitted query
      // 1) Saves the avr dag to the PlanStore and
      // converts the avro dag to the logical and execution dag
      planStore.saveAvroDag(tuple);
      final String queryId = tuple.getKey();


      // Update group information
      final String realGroupId = tuple.getValue().getSuperGroupId();
      final String groupId = Long.toString(groupIdCounter.getAndIncrement());

      final String subGroupId = tuple.getValue().getSubGroupId();


      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "Create Query [gid: {0}, sgid: {1}, qid: {2}]",
            new Object[]{groupId, subGroupId, queryId});
      }

      final JavaConfigurationBuilder bb = Tang.Factory.getTang().newConfigurationBuilder();
      bb.bindNamedParameter(GroupId.class, groupId);
      final Group group = Tang.Factory.getTang().newInjector(bb.build()).getInstance(Group.class);

      if (groupMap.get(realGroupId) == null) {
        // Add new group id, if it doesn't exist
        final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

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
        injector.bindVolatileInstance(MQTTResource.class, mqttSharedResource);
        injector.bindVolatileInstance(KafkaSharedResource.class, kafkaSharedResource);
        injector.bindVolatileInstance(NettySharedResource.class, nettySharedResource);
        injector.bindVolatileInstance(QueryInfoStore.class, planStore);

        if (!mergingEnabled) {
          injector.bindVolatileInstance(DagGenerator.class, dagGenerator);
        }

        final MetaGroup metaGroup = injector.getInstance(MetaGroup.class);

        if (groupMap.putIfAbsent(realGroupId, new Tuple<>(metaGroup, new AtomicBoolean(false))) == null) {
          LOG.log(Level.FINE, "Create Group: {0}", new Object[]{realGroupId});
        }
      }

      // Add a query as a group (not group-aware)
      final Tuple<MetaGroup, AtomicBoolean> mGroup = groupMap.get(realGroupId);
      // Add group here
      groupAllocationTableModifier.addEvent(
          new WritingEvent(WritingEvent.EventType.GROUP_ADD, group));

      final Query query = new DefaultQueryImpl(queryId);
      groupAllocationTableModifier.addEvent(new WritingEvent(WritingEvent.EventType.QUERY_ADD,
          new Tuple<>(group, query)));

      // Start the submitted dag
      final DAG<ConfigVertex, MISTEdge> configDag = configDagGenerator.generate(tuple.getValue());
      mGroup.getKey().getQueryStarter().start(queryId, query, configDag, tuple.getValue().getJarFilePaths());

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
   * @param tuple a pair of the query id and the avro dag
   * @return submission result
   */
  @Override
  public QueryControlResult createBatch(final Tuple<List<String>, AvroDag> tuple) {
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
    eventProcessorManager.close();
  }

  /**
   * Deletes queries from MIST.
   */
  @Override
  public QueryControlResult delete(final String groupId, final String queryId) {
    groupMap.get(groupId).getKey().getQueryRemover().deleteQuery(queryId);
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryId);
    queryControlResult.setIsSuccess(true);
    queryControlResult.setMsg(ResultMessage.deleteSuccess(queryId));
    return queryControlResult;
  }

  @Override
  public GroupSourceManager getGroupSourceManager(final String groupId) {
    return null;
  }
}