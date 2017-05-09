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

import edu.snu.mist.core.task.OperatorChain;
import edu.snu.mist.core.task.OperatorChainManager;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class picks a query, which has more than one events to be processed, from the active query set randomly.
 * This prevents picking queries which have no events, thus saving CPU cycles compared to RandomlyPickManager.
 * This class uses queues to provide maximum concurrency and efficiency.
 *
 * This also reschedules the group if it is activated from deactivated status.
 */
public final class GroupActiveOperatorChainPickManager implements OperatorChainManager {
  private static final Logger LOG = Logger.getLogger(GroupActiveOperatorChainPickManager.class.getName());

  /**
   * A working queue which contains queries that will be processed soon.
   */
  private final Queue<OperatorChain> activeQueryQueue;

  /**
   * Next group selector that schedules the group.
   */
  private final NextGroupSelector nextGroupSelector;

  /**
   * Group info future that is related to this operator chain manager.
   */
  private final InjectionFuture<GlobalSchedGroupInfo> groupInfoFuture;

  @Inject
  private GroupActiveOperatorChainPickManager(final InjectionFuture<GlobalSchedGroupInfo> groupInfoFuture,
                                              final NextGroupSelector nextGroupSelector) {
    this.activeQueryQueue = new LinkedList<>();
    this.nextGroupSelector = nextGroupSelector;
    this.groupInfoFuture = groupInfoFuture;
  }

  /**
   * Insert a new operator chain into the manager. This method is called when the
   * queue of a operatorChain just becomes not empty by getting an event, or the queue is still not empty
   * after thread's processing an event.
   * @param operatorChain a operatorChain to be inserted
   */
  @Override
  public void insert(final OperatorChain operatorChain) {
    final GlobalSchedGroupInfo groupInfo = groupInfoFuture.get();

    synchronized (groupInfo) {
      if (groupInfo.getStatus() == GlobalSchedGroupInfo.Status.INACTIVE) {
        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "{0}: Inactive -> Active group {1}",
              new Object[]{Thread.currentThread().getName(), groupInfo});
        }

        // Reschedule if the group is inactive
        // because the group is removed from rb-tree of the next group selector
        activeQueryQueue.add(operatorChain);
        groupInfo.setStatus(GlobalSchedGroupInfo.Status.ACTIVE);
        nextGroupSelector.reschedule(groupInfo, false);
      } else {
        activeQueryQueue.add(operatorChain);
      }
    }
  }

  /**
   * Delete an operator from the manager.
   * This method should be only called when the queue is permanently removed from the system.
   * @param operatorChain a operatorChain to be deleted.
   */
  @Override
  public void delete(final OperatorChain operatorChain) {
    final GlobalSchedGroupInfo groupInfo = groupInfoFuture.get();

    synchronized (groupInfo) {
      activeQueryQueue.remove(operatorChain);
    }
  }

  /**
   * Pick an operator chain from the manager and removes it from the working queue.
   * @return picked operator chain if there is. null when there is no active queries to be processed.
   */
  @Override
  public OperatorChain pickOperatorChain() {
    final GlobalSchedGroupInfo groupInfo = groupInfoFuture.get();
    synchronized (groupInfo) {
      final OperatorChain activeQuery = activeQueryQueue.poll();
      if (activeQuery == null) {
        // Make group inactive if it is empty
        groupInfo.setStatus(GlobalSchedGroupInfo.Status.INACTIVE);
      }
      return activeQuery;
    }
  }
}