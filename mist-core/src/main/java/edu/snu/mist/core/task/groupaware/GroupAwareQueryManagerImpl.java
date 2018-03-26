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

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.parameters.MasterHostname;
import edu.snu.mist.core.parameters.TaskHostname;
import edu.snu.mist.core.parameters.TaskToMasterPort;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.core.parameters.GroupId;
import edu.snu.mist.core.sources.parameters.PeriodicCheckpointPeriod;
import edu.snu.mist.core.shared.KafkaSharedResource;
import edu.snu.mist.core.shared.MQTTResource;
import edu.snu.mist.core.shared.NettySharedResource;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.checkpointing.CheckpointManager;
import edu.snu.mist.core.task.groupaware.parameters.ApplicationIdentifier;
import edu.snu.mist.core.task.groupaware.parameters.JarFilePath;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.QueryCheckpoint;
import edu.snu.mist.formats.avro.QueryControlResult;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
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
public final class GroupAwareQueryManagerImpl implements QueryManager {

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
   * Application map.
   */
  private final ApplicationMap applicationMap;

  /**
   * Group map.
   */
  private final GroupMap groupMap;

  /**
   * Event processor manager.
   */
  private final EventProcessorManager eventProcessorManager;

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

  /**
   * TODO: This should be generate globally unique numbers.
   */
  private final AtomicLong applicationNum = new AtomicLong(0);

  /**
   * The checkpoint period.
   */
  private final long checkpointPeriod;

  /**
   * The avro proxy to master.
   */
  private final TaskToMasterMessage proxyToMaster;

  /**
   * The hostname of this task seen from MistMaster.
   */
  private final String taskHostname;

  /**
   * The checkpoint manager.
   */
  private final CheckpointManager checkpointManager;

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private GroupAwareQueryManagerImpl(final ScheduledExecutorServiceWrapper schedulerWrapper,
                                     final QueryInfoStore planStore,
                                     final EventProcessorManager eventProcessorManager,
                                     final ConfigDagGenerator configDagGenerator,
                                     final MQTTResource mqttSharedResource,
                                     final KafkaSharedResource kafkaSharedResource,
                                     final NettySharedResource nettySharedResource,
                                     final DagGenerator dagGenerator,
                                     final GroupAllocationTableModifier groupAllocationTableModifier,
                                     final ApplicationMap applicationMap,
                                     @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod,
                                     @Parameter(MasterHostname.class) final String masterHostAddress,
                                     @Parameter(TaskToMasterPort.class) final int taskToMasterPort,
                                     @Parameter(TaskHostname.class) final String taskHostname,
                                     final GroupMap groupMap,
                                     final CheckpointManager checkpointManager) throws IOException {

    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.eventProcessorManager = eventProcessorManager;
    this.configDagGenerator = configDagGenerator;
    this.mqttSharedResource = mqttSharedResource;
    this.kafkaSharedResource = kafkaSharedResource;
    this.nettySharedResource = nettySharedResource;
    this.dagGenerator = dagGenerator;
    this.groupAllocationTableModifier = groupAllocationTableModifier;
    this.applicationMap = applicationMap;
    this.groupMap = groupMap;
    this.checkpointPeriod = checkpointPeriod;
    this.proxyToMaster = AvroUtils.createAvroProxy(TaskToMasterMessage.class,
        new InetSocketAddress(masterHostAddress, taskToMasterPort));
    this.taskHostname = taskHostname;
    this.checkpointManager = checkpointManager;
    this.checkpointManager.startCheckpointing();
  }

  /**
   * Start a submitted query.
   * It converts the avro operator chain dag (query) to the execution dag,
   * and executes the sources in order to receives data streams.
   * Before the queries are executed, it stores the avro  dag into disk.
   * We can regenerate the queries from the stored avro dag.
   * @param avroDag the avro dag
   * @return submission result
   */
  @Override
  public QueryControlResult create(final AvroDag avroDag) {
    return createWithCheckpointedStates(avroDag, null);
  }

  /**
   * Start a submitted query with the checkpointed states.
   * @param avroDag the avro dag
   * @param checkpointedStates the checkpointed states
   * @return
   */
  @Override
  public QueryControlResult createWithCheckpointedStates(final AvroDag avroDag,
                                                         final QueryCheckpoint checkpointedStates) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    final String queryId = avroDag.getQueryId();
    queryControlResult.setQueryId(queryId);
    try {
      // Create the submitted query
      // 1) Saves the avr dag and converts the avro dag to the logical and execution dag
      checkpointManager.storeQuery(avroDag);
      // Update app information
      final String appId = avroDag.getAppId();

      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "Create Query [aid: {0}, qid: {2}]",
            new Object[]{appId, queryId});
      }

      if (!applicationMap.containsKey(appId)) {
        createApplication(appId, avroDag.getJarPaths());
      }

      final ApplicationInfo applicationInfo = applicationMap.get(appId);
      final DAG<ConfigVertex, MISTEdge> configDag;
      if (checkpointedStates == null) {
        configDag = configDagGenerator.generate(avroDag);
      } else {
        configDag = configDagGenerator.generateWithCheckpointedStates(avroDag, checkpointedStates);
      }
      // Waiting for group information being added
      while (applicationInfo.getGroups().isEmpty()) {
        Thread.sleep(100);
      }
      final Query query = createAndStartQuery(queryId, applicationInfo, configDag);

      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(queryId));
      return queryControlResult;
    } catch (final Exception e) {
      e.printStackTrace();
      // [MIST-345] We need to release all of the information that is required for the query when it fails.
      LOG.log(Level.SEVERE, "An exception occurred while starting {0} query: {1}",
          new Object[] {queryId, e.toString()});

      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(e.getMessage());
      return queryControlResult;
    }
  }

  @Override
  public Query createAndStartQuery(final String queryId,
                                   final ApplicationInfo applicationInfo,
                                   final DAG<ConfigVertex, MISTEdge> configDag)
      throws ClassNotFoundException, IOException {
    final Query query = new DefaultQueryImpl(queryId);
    groupAllocationTableModifier.addEvent(new WritingEvent(WritingEvent.EventType.QUERY_ADD,
        new Tuple<>(applicationInfo, query)));
    // Start the submitted dag
    // TODO : [MIST-1016] get the appropriate Group for this applicationInfo.
    final Group group = applicationInfo.getRandomGroup();
    group.getQueryStarter().start(queryId, query, configDag, applicationInfo.getJarFilePath());
    return query;
  }

  @Override
  public ApplicationInfo createApplication(final String appId, final List<String> paths) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

    jcb.bindNamedParameter(ApplicationIdentifier.class, appId);
    // TODO: Submit a single jar instead of list of jars
    jcb.bindNamedParameter(JarFilePath.class, paths.get(0));
    // Get a unique group id from the MistMaster
    try {
      final String groupId = proxyToMaster.createGroup(taskHostname,
          GroupStats.newBuilder()
              .setGroupCpuLoad(0.0)
              .setGroupQueryNum(1)
              .setAppId(appId)
              .build());
      jcb.bindNamedParameter(GroupId.class, groupId);
    } catch (final AvroRemoteException e) {
      e.printStackTrace();
    }
    jcb.bindNamedParameter(PeriodicCheckpointPeriod.class, String.valueOf(checkpointPeriod));

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(MQTTResource.class, mqttSharedResource);
    injector.bindVolatileInstance(KafkaSharedResource.class, kafkaSharedResource);
    injector.bindVolatileInstance(NettySharedResource.class, nettySharedResource);
    injector.bindVolatileInstance(QueryInfoStore.class, planStore);

    final ApplicationInfo applicationInfo = injector.getInstance(ApplicationInfo.class);

    applicationMap.putIfAbsent(appId, applicationInfo);

    final Group group = injector.getInstance(Group.class);
    groupAllocationTableModifier.addEvent(
            new WritingEvent(WritingEvent.EventType.GROUP_ADD, new Tuple<>(applicationInfo, group)));

    return applicationInfo;
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
    groupMap.get(groupId).getQueryRemover().deleteQuery(queryId);
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryId);
    queryControlResult.setIsSuccess(true);
    queryControlResult.setMsg(ResultMessage.deleteSuccess(queryId));
    return queryControlResult;
  }
}
