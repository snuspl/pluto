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

import edu.snu.mist.core.sources.parameters.PeriodicCheckpointPeriod;
import edu.snu.mist.core.task.Query;
import edu.snu.mist.core.task.QueryManager;
import edu.snu.mist.core.task.QueryRemover;
import edu.snu.mist.core.task.groupaware.ApplicationInfo;
import edu.snu.mist.core.task.groupaware.ApplicationMap;
import edu.snu.mist.core.task.groupaware.Group;
import edu.snu.mist.core.task.groupaware.GroupAllocationTableModifier;
import edu.snu.mist.core.task.groupaware.GroupMap;
import edu.snu.mist.core.task.groupaware.WritingEvent;
import edu.snu.mist.core.task.stores.GroupCheckpointStore;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.CheckpointResult;
import edu.snu.mist.formats.avro.QueryCheckpoint;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
  private final InjectionFuture<GroupAllocationTableModifier> tableModifierFuture;

  /**
   * The query manager for the task. Use InjectionFuture to avoid cyclic injection.
   **/
  private final InjectionFuture<QueryManager> queryManagerFuture;

  /**
   * The checkpoint period.
   */
  private final long checkpointPeriod;

  @Inject
  private DefaultCheckpointManagerImpl(final ApplicationMap applicationMap,
                                       final GroupMap groupMap,
                                       final GroupCheckpointStore groupCheckpointStore,
                                       final InjectionFuture<GroupAllocationTableModifier> tableModifierFuture,
                                       final InjectionFuture<QueryManager> queryManagerFuture,
                                       @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod) {
    this.applicationMap = applicationMap;
    this.groupMap = groupMap;
    this.checkpointStore = groupCheckpointStore;
    this.tableModifierFuture = tableModifierFuture;
    this.checkpointPeriod = checkpointPeriod;
    this.queryManagerFuture = queryManagerFuture;
    if (checkpointPeriod == 0) {
      LOG.log(Level.INFO, "checkpointing is not turned on");
    } else {
      LOG.log(Level.INFO, "Start checkpointing... Period = {0}", checkpointPeriod);
      final ScheduledExecutorService checkpointScheduler = Executors.newSingleThreadScheduledExecutor();
      final CheckpointRunner runner = new CheckpointRunner(groupMap, this);
      checkpointScheduler.scheduleAtFixedRate(runner, checkpointPeriod, checkpointPeriod, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public boolean storeQuery(final Group group,
                            final AvroDag avroDag) {
    return checkpointStore.saveQuery(group, avroDag);
  }

  @Override
  public void recoverGroup(final String groupId) throws IOException {
    Map<String, QueryCheckpoint> queryCheckpointMap;
    final List<AvroDag> dagList;
    final QueryManager queryManager = queryManagerFuture.get();
    try {
      // Load the checkpointed states and the query lists.
      queryCheckpointMap = checkpointStore.loadSavedGroupState(groupId).getQueryCheckpointMap();
      // Load the queries.
    } catch (final FileNotFoundException ie) {
      LOG.log(Level.WARNING, "Checkpoint is not found for group {0}.", new Object[]{groupId});
      // Insert an empty map to prevent null point exception.
      queryCheckpointMap = new HashMap<>();
    }

    final List<String> queryIdListInGroup = checkpointStore.loadSaveGroupQueryInfo(groupId);
    dagList = checkpointStore.loadSavedQueries(queryIdListInGroup);

    for (final AvroDag avroDag : dagList) {
      // Get the checkpoint for each dag. If there is no checkpoint for the query, it returns null.
      final QueryCheckpoint queryCheckpoint = queryCheckpointMap.get(avroDag.getQueryId());
      // Recover each query in the group.
      queryManager.createQueryWithCheckpoint(avroDag, queryCheckpoint);
    }
  }

  @Override
  public boolean checkpointGroup(final String groupId) {
    final Group group = groupMap.get(groupId);
    if (group == null) {
      LOG.log(Level.WARNING, "There is no such group {0}.",
          new Object[] {groupId});
      return false;
    }
    final CheckpointResult result =
        checkpointStore.checkpointGroupStates(new Tuple<>(groupId, group));
    LOG.log(Level.INFO, "Checkpoint started for groupId : {0}, result : {1}",
        new Object[]{groupId, result.getIsSuccess()});
    return result.getIsSuccess();
  }

  @Override
  public boolean createGroupQueryInfoFile(final Group group) {
    return checkpointStore.createGroupQueryInfoFile(group);
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
    tableModifierFuture.get().addEvent(
        new WritingEvent(WritingEvent.EventType.GROUP_REMOVE, group));
  }

  @Override
  public Group getGroup(final String groupId) {
    return groupMap.get(groupId);
  }

  @Override
  public ApplicationInfo getApplication(final String appId) {
    return applicationMap.get(appId);
  }

  /**
   * A runnable class for automatic periodic checkpointing.
   */
  private final class CheckpointRunner implements Runnable {

    /**
     * The group map.
     */
    private final GroupMap groupMap;

    /**
     * The checkpoint manager.
     */
    private final CheckpointManager checkpointManager;

    public CheckpointRunner(final GroupMap groupMap,
                            final CheckpointManager checkpointManager) {
      this.groupMap = groupMap;
      this.checkpointManager = checkpointManager;
    }

    @Override
    public synchronized void run() {
      // Checkpoint the all registered groups.
      for (final Map.Entry<String, Group> groupEntry : groupMap.entrySet()) {
        final String groupId = groupEntry.getKey();
        checkpointManager.checkpointGroup(groupId);
      }
    }
  }
}
