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

import edu.snu.mist.core.task.Query;
import edu.snu.mist.core.task.QueryManager;
import edu.snu.mist.core.task.groupaware.*;
import edu.snu.mist.core.task.stores.GroupCheckpointStore;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.CheckpointResult;
import edu.snu.mist.formats.avro.QueryCheckpoint;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
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
   * The query manager for the task. Use InjectionFuture to avoid cyclic injection.
   **/
  private final InjectionFuture<QueryManager> queryManagerFuture;

  @Inject
  private DefaultCheckpointManagerImpl(final ApplicationMap applicationMap,
                                       final GroupMap groupMap,
                                       final GroupCheckpointStore groupCheckpointStore,
                                       final GroupAllocationTableModifier groupAllocationTableModifier,
                                       final InjectionFuture<QueryManager> queryManagerFuture) {
    this.applicationMap = applicationMap;
    this.groupMap = groupMap;
    this.checkpointStore = groupCheckpointStore;
    this.groupAllocationTableModifier = groupAllocationTableModifier;
    this.queryManagerFuture = queryManagerFuture;
  }

  @Override
  public boolean storeQuery(final AvroDag avroDag) {
    return checkpointStore.saveQuery(avroDag);
  }

  @Override
  public void recoverGroup(final String groupId) throws IOException {
    final Map<String, QueryCheckpoint> queryCheckpointMap;
    final List<AvroDag> dagList;
    final QueryManager queryManager = queryManagerFuture.get();
    try {
      // Load the queries.
      final List<String> queryIdListInGroup = new ArrayList<>();
      for (final Query query : groupMap.get(groupId).getQueries()) {
        queryIdListInGroup.add(query.getId());
      }
      dagList = checkpointStore.loadSavedQueries(queryIdListInGroup);
      // Load the states.
      queryCheckpointMap = checkpointStore.loadSavedGroupState(groupId).getQueryCheckpointMap();
    } catch (final FileNotFoundException ie) {
      LOG.log(Level.WARNING, "Failed in loading app {0}, this app may not exist in the checkpoint store.",
          new Object[]{groupId});
      return;
    }

    for (final AvroDag avroDag : dagList) {
      final QueryCheckpoint queryCheckpoint = queryCheckpointMap.get(avroDag.getQueryId());
      // Recover each query in the group.
      queryManager.createWithCheckpointedStates(avroDag, queryCheckpoint);
    }
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
        checkpointStore.checkpointGroupStates(new Tuple<>(groupId, group));
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
    group.getQueryRemover().deleteAllQueries();
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
