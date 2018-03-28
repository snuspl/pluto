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
import edu.snu.mist.core.sources.parameters.PeriodicCheckpointPeriod;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.groupaware.*;
import edu.snu.mist.core.task.stores.GroupCheckpointStore;
import edu.snu.mist.formats.avro.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DefaultCheckpointManagerImpl implements CheckpointManager {

  private static final Logger LOG = Logger.getLogger(DefaultCheckpointManagerImpl.class.getName());

  /**
   * A map containing information about each application.
   */
  private final ApplicationMap applicationMap;

  /**
   * A map containing information about each group.
   */
  private final GroupMap groupMap;

  /**
   * A group checkpoint store.
   */
  private final GroupCheckpointStore checkpointStore;

  /**
   * A modifier for the group allocation table.
   */
  private final GroupAllocationTableModifier groupAllocationTableModifier;

  /**
   * The query manager for the task.
   */
  private final QueryManager queryManager;

  /**
   * The checkpoint period.
   */
  private final long checkpointPeriod;

  @Inject
  private DefaultCheckpointManagerImpl(final ApplicationMap applicationMap,
                                       final GroupMap groupMap,
                                       final GroupCheckpointStore groupCheckpointStore,
                                       final GroupAllocationTableModifier groupAllocationTableModifier,
                                       final QueryManager queryManager,
                                       @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod) {
    this.applicationMap = applicationMap;
    this.groupMap = groupMap;
    this.checkpointStore = groupCheckpointStore;
    this.groupAllocationTableModifier = groupAllocationTableModifier;
    this.queryManager = queryManager;
    this.checkpointPeriod = checkpointPeriod;
    if (checkpointPeriod == 0) {
      LOG.log(Level.INFO, "checkpointing is not turned on");
    }
  }

  @Override
  public void recoverGroup(final String groupId) throws IOException {
    final GroupCheckpoint checkpoint;
    try {
      checkpoint = checkpointStore.loadGroupCheckpoint(groupId);
    } catch (final FileNotFoundException ie) {
      LOG.log(Level.WARNING, "Failed in loading app {0}, this app may not exist in the checkpoint store.",
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
          LOG.log(Level.FINE, "Recover Query [groupId: {0}, qid: {1}]",
              new Object[]{groupId, queryId});
        }

        // Add the query info to the queryManager.
        final List<String> jarFilePaths = checkpoint.getJarFilePaths();
        final String appId = checkpoint.getApplicationId();
        final ApplicationInfo applicationInfo;
        if (applicationMap.containsKey(appId)) {
          applicationInfo = applicationMap.get(appId);
        } else {
          applicationInfo =
              queryManager.createApplication(checkpoint.getApplicationId(), jarFilePaths);
        }
        // Waiting for group information being added
        while (applicationInfo.getGroups().isEmpty()) {
          Thread.sleep(100);
        }
        // Start the submitted dag
        queryManager.createAndStartQuery(queryId, applicationInfo, configDag);
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
  public boolean checkpointGroup(final String groupId) {
    LOG.log(Level.INFO, "Checkpoint started for groupId : {0}", groupId);
    final Group group = groupMap.get(groupId);
    if (group == null) {
      LOG.log(Level.WARNING, "There is no such group {0}.",
          new Object[] {groupId});
      return false;
    }
    final CheckpointResult result =
        checkpointStore.saveGroupCheckpoint(new Tuple<>(groupId, group));
    return result.getIsSuccess();
  }

  @Override
  public void deleteGroup(final String groupId) {
    final Group group = groupMap.get(groupId);
    if (group == null) {
      LOG.log(Level.WARNING, "There is no such group {0}.",
          new Object[] {groupId});
      return;
    }
    final QueryRemover remover = group.getApplicationInfo().getQueryRemover();
    for (final Query query : group.getQueries()) {
      remover.deleteQuery(query.getId());
    }
    applicationMap.remove(groupId);
    groupAllocationTableModifier.addEvent(
        new WritingEvent(WritingEvent.EventType.GROUP_REMOVE_ALL, null));
  }

  @Override
  public Group getGroup(final String groupId) {
    return groupMap.get(groupId);
  }

  @Override
  public ApplicationInfo getApplication(final String appId) {
    return applicationMap.get(appId);
  }
}
