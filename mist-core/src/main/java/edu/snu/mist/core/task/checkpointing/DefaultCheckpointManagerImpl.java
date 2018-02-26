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
package edu.snu.mist.core.task.checkpointing;

import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.common.shared.KafkaSharedResource;
import edu.snu.mist.common.shared.MQTTResource;
import edu.snu.mist.common.shared.NettySharedResource;
import edu.snu.mist.core.driver.parameters.MergingEnabled;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.groupaware.*;
import edu.snu.mist.core.task.merging.*;
import edu.snu.mist.core.task.stores.MetaGroupCheckpointStore;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DefaultCheckpointManagerImpl implements CheckpointManager {

  private static final Logger LOG = Logger.getLogger(DefaultCheckpointManagerImpl.class.getName());

  /**
   * A map containing information about each group.
   */
  private final GroupMap groupMap;

  /**
   * A group checkpoint store.
   */
  private final MetaGroupCheckpointStore checkpointStore;

  /**
   * A modifier for the group allocation table.
   */
  private final GroupAllocationTableModifier groupAllocationTableModifier;

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

  /**
   * A plan store.
   */
  private final QueryInfoStore planStore;

  private final DagGenerator dagGenerator;

  /**
   * Merging enabled or not.
   */
  private final boolean mergingEnabled;

  @Inject
  private DefaultCheckpointManagerImpl(final GroupMap groupMap,
                                       final MetaGroupCheckpointStore metaGroupCheckpointStore,
                                       final GroupAllocationTableModifier groupAllocationTableModifier,
                                       final MQTTResource mqttSharedResource,
                                       final KafkaSharedResource kafkaSharedResource,
                                       final NettySharedResource nettySharedResource,
                                       final QueryInfoStore planStore,
                                       final DagGenerator dagGenerator,
                                       @Parameter(MergingEnabled.class) final boolean mergingEnabled) {
    this.groupMap = groupMap;
    this.checkpointStore = metaGroupCheckpointStore;
    this.groupAllocationTableModifier = groupAllocationTableModifier;
    this.mqttSharedResource = mqttSharedResource;
    this.kafkaSharedResource = kafkaSharedResource;
    this.nettySharedResource = nettySharedResource;
    this.planStore = planStore;
    this.dagGenerator = dagGenerator;
    this.mergingEnabled = mergingEnabled;
  }

  @Override
  public void recoverGroup(final String groupId) throws IOException {
    final MetaGroupCheckpoint checkpoint;
    try {
      checkpoint = checkpointStore.loadMetaGroupCheckpoint(groupId);
    } catch (final FileNotFoundException ie) {
      LOG.log(Level.WARNING, "Failed in loading group {0}, this group may not exist in the checkpoint store.",
          new Object[]{groupId});
      return;
    }

    // Construct a ConfigDag for each query and submit it.
    // The submission process is almost the same to create(), except that it uses a ConfigDag instead of an AvroDag.
    for (final Map.Entry<String, AvroConfigDag> entry : checkpoint.getAvroConfigDags().entrySet()) {
      final String queryId = entry.getKey();
      LOG.log(Level.INFO, "Query with id {0} is being submitted during recovery of group id {1}",
          new Object[]{queryId, groupId});
      final AvroConfigDag dag = entry.getValue();
      final List<AvroConfigVertex> vertexList = dag.getAvroConfigVertices();
      final List<AvroConfigMISTEdge> edgeList = dag.getAvroConfigMISTEdges();

      // Construct a ConfigDag(DAG<ConfigVertex, MISTEdge>) from an AvroConfigDag.
      final DAG<ConfigVertex, MISTEdge> configDag = new AdjacentListDAG<>();

      for (final AvroConfigVertex vertex : vertexList) {
        configDag.addVertex(convertToConfigVertex(vertex));
      }

      for (final AvroConfigMISTEdge edge : edgeList) {
        final AvroConfigVertex fromVertex = vertexList.get(edge.getFromVertexIndex());
        final AvroConfigVertex toVertex = vertexList.get(edge.getToVertexIndex());
        configDag.addEdge(convertToConfigVertex(fromVertex), convertToConfigVertex(toVertex),
            new MISTEdge(edge.getDirection(), edge.getIndex()));
      }

      // Submit the query with the ConfigDag. This is almost the same to create().
      try {
        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "Recover Query [gid: {0}, qid: {1}]",
              new Object[]{groupId, queryId});
        }

        if (groupMap.get(groupId) == null) {
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
          // TODO[DELETE] end: for test

          final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
          injector.bindVolatileInstance(MQTTResource.class, mqttSharedResource);
          injector.bindVolatileInstance(KafkaSharedResource.class, kafkaSharedResource);
          injector.bindVolatileInstance(NettySharedResource.class, nettySharedResource);
          injector.bindVolatileInstance(QueryInfoStore.class, planStore);

          if (!mergingEnabled) {
            injector.bindVolatileInstance(DagGenerator.class, dagGenerator);
          }

          final MetaGroup metaGroup = injector.getInstance(MetaGroup.class);

          if (groupMap.putIfAbsent(groupId, new Tuple<>(metaGroup, new AtomicBoolean(false))) == null) {
            LOG.log(Level.FINE, "Create Group: {0}", new Object[]{groupId});
            final Group group = injector.getInstance(Group.class);
            groupAllocationTableModifier.addEvent(
                new WritingEvent(WritingEvent.EventType.GROUP_ADD, new Tuple<>(metaGroup, group)));

            final Tuple<MetaGroup, AtomicBoolean> mGroup = groupMap.get(groupId);
            synchronized (mGroup) {
              mGroup.getValue().set(true);
              mGroup.notifyAll();
            }

            /*
            synchronized (metaGroup.getGroups()) {
              metaGroup.getGroups().add(group);
              eventProcessorManager.addGroup(group);
            }
            */
          }
        }

        final Tuple<MetaGroup, AtomicBoolean> mGroup = groupMap.get(groupId);
        synchronized (mGroup) {
          if (!mGroup.getValue().get()) {
            mGroup.wait();
          }
        }

        final Query query = new DefaultQueryImpl(queryId);
        groupAllocationTableModifier.addEvent(new WritingEvent(WritingEvent.EventType.QUERY_ADD,
            new Tuple<>(mGroup.getKey(), query)));

        // Start the submitted dag
        mGroup.getKey().getQueryStarter().start(queryId, query, configDag, checkpoint.getJarFilePaths());
      } catch (final Exception e) {
        e.printStackTrace();
        // [MIST-345] We need to release all of the information that is required for the query when it fails.
        LOG.log(Level.SEVERE, "An exception occurred while recovering {0} query: {1}",
            new Object[] {groupId, e.toString()});
      }
    }
  }

  private ConfigVertex convertToConfigVertex(final AvroConfigVertex vertex) {
    final ExecutionVertex.Type type;
    if (vertex.getType() == AvroConfigVertexType.SOURCE) {
      type = ExecutionVertex.Type.SOURCE;
    } else if (vertex.getType() == AvroConfigVertexType.OPERATOR) {
      type = ExecutionVertex.Type.OPERATOR;
    } else {
      type = ExecutionVertex.Type.SINK;
    }
    return new ConfigVertex(vertex.getId(), type,
        vertex.getConfiguration(), vertex.getState(), vertex.getLatestCheckpointTimestamp());
  }

  @Override
  public void checkpointGroup(final String groupId) {
    LOG.log(Level.INFO, "Checkpoint started for groupId : {0}", groupId);
    final Tuple<MetaGroup, AtomicBoolean> tuple = groupMap.get(groupId);
    if (tuple == null) {
      LOG.log(Level.WARNING, "There is no such group {0}.",
          new Object[] {groupId});
      return;
    }
    final MetaGroup group = tuple.getKey();
    checkpointStore.saveMetaGroupCheckpoint(new Tuple<>(groupId, group.checkpoint()));
  }

  @Override
  public void deleteGroup(final String groupId) {
    final Tuple<MetaGroup, AtomicBoolean> tuple = groupMap.get(groupId);
    if (tuple == null) {
      LOG.log(Level.WARNING, "There is no such group {0}.",
          new Object[] {groupId});
      return;
    }
    tuple.getKey().getQueryRemover().deleteAllQueries();
    groupMap.remove(groupId);
    groupAllocationTableModifier.addEvent(
        new WritingEvent(WritingEvent.EventType.GROUP_REMOVE_ALL, null));
  }

  @Override
  public MetaGroup getGroup(final String groupId) {
    return groupMap.get(groupId).getKey();
  }
}
