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
import edu.snu.mist.core.task.ConfigVertex;
import edu.snu.mist.core.task.ExecutionVertex;
import edu.snu.mist.core.task.Query;
import edu.snu.mist.core.task.QueryManager;
import edu.snu.mist.core.task.groupaware.GroupAllocationTableModifier;
import edu.snu.mist.core.task.groupaware.GroupMap;
import edu.snu.mist.core.task.groupaware.MetaGroup;
import edu.snu.mist.core.task.groupaware.WritingEvent;
import edu.snu.mist.core.task.stores.MetaGroupCheckpointStore;
import edu.snu.mist.formats.avro.*;
import org.apache.reef.io.Tuple;

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
   * The query manager for the task.
   */
  private final QueryManager queryManager;

  @Inject
  private DefaultCheckpointManagerImpl(final GroupMap groupMap,
                                       final MetaGroupCheckpointStore metaGroupCheckpointStore,
                                       final GroupAllocationTableModifier groupAllocationTableModifier,
                                       final QueryManager queryManager) {
    this.groupMap = groupMap;
    this.checkpointStore = metaGroupCheckpointStore;
    this.groupAllocationTableModifier = groupAllocationTableModifier;
    this.queryManager = queryManager;
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

        // Add the query info to the queryManager.
        final List<String> jarFilePaths = checkpoint.getJarFilePaths();
        final Query query = queryManager.addNewQueryInfo(groupId, queryId, jarFilePaths);
        final Tuple<MetaGroup, AtomicBoolean> mGroup = groupMap.get(groupId);

        // Start the submitted dag
        mGroup.getKey().getQueryStarter().start(queryId, query, configDag, jarFilePaths);
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

  @Override
  public void close() throws Exception {
    checkpointStore.close();
  }
}
