
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
package edu.snu.mist.core.task.globalsched.roundrobin.polling;

import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfoMap;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import org.apache.reef.wake.impl.PubSubEventHandler;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This schedules groups according to the weighted round-robin policy.
 * It wakes up inactive groups by polling based approach.
 */
public final class WrrPollingNextGroupSelector implements NextGroupSelector {

  private static final Logger LOG = Logger.getLogger(WrrPollingNextGroupSelector.class.getName());

  /**
   * A activeQueue for round-robin.
   */
  private final BlockingQueue<GlobalSchedGroupInfo> activeQueue;

  /**
   * Pub/sub handler for group event.
   */
  private final PubSubEventHandler pubSubEventHandler;

  /**
   * Inactive group checker.
   */
  private final InactiveGroupChecker inactiveGroupChecker;

  /**
   * Polling period.
   */
  private final long pollingPeriod;

  /**
   * Polling executor service.
   */
  private final ScheduledExecutorService pollingService;

  /**
   * Group status map.
   */
  private final Map<GlobalSchedGroupInfo, AtomicBoolean> groupStatusMap;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  WrrPollingNextGroupSelector(final MistPubSubEventHandler pubSubEventHandler,
                              final GlobalSchedGroupInfoMap globalSchedGroupInfoMap,
                              final InactiveGroupChecker inactiveGroupChecker,
                              final long pollingPeriod) {
    this.activeQueue = new LinkedBlockingQueue<>();
    this.inactiveGroupChecker = inactiveGroupChecker;
    this.pollingPeriod = pollingPeriod;
    this.groupStatusMap = new ConcurrentHashMap<>();
    this.pollingService = Executors.newSingleThreadScheduledExecutor();

    this.pollingService.scheduleAtFixedRate(() -> {
      // Check the inactive groups in polling approach.
      try {
        final Set<Map.Entry<GlobalSchedGroupInfo, AtomicBoolean>> entries = groupStatusMap.entrySet();
        for (final Map.Entry<GlobalSchedGroupInfo, AtomicBoolean> entry : entries) {
          final GlobalSchedGroupInfo groupInfo = entry.getKey();
          if (groupInfo.getOperatorChainManager().size() > 0 && entry.getValue().compareAndSet(false, true)) {
            activeQueue.add(entry.getKey());
          }
        }
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }, pollingPeriod, pollingPeriod, TimeUnit.MILLISECONDS);

    initialize(globalSchedGroupInfoMap);
    this.pubSubEventHandler = pubSubEventHandler.getPubSubEventHandler();
    this.pubSubEventHandler.subscribe(GroupEvent.class, this);
  }

  /**
   * Initialize the queue.
   * @param globalSchedGroupInfoMap
   */
  private void initialize(final GlobalSchedGroupInfoMap globalSchedGroupInfoMap) {
    for (final GlobalSchedGroupInfo groupInfo : globalSchedGroupInfoMap.values()) {
      addGroup(groupInfo);
    }
  }

  /**
   * Add the group to the activeQueue.
   * @param groupInfo group info
   */
  private void addGroup(final GlobalSchedGroupInfo groupInfo) {
    groupStatusMap.put(groupInfo, new AtomicBoolean(true));
    activeQueue.add(groupInfo);

    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "{0} add Group {1} to Queue: {2}",
          new Object[]{Thread.currentThread().getName(), groupInfo, activeQueue});
    }
  }

  /**
   * Remove the group.
   * @param groupInfo group info
   */
  private void removeGroup(final GlobalSchedGroupInfo groupInfo) {
    groupStatusMap.remove(groupInfo);
    activeQueue.remove(groupInfo);
  }

  @Override
  public GlobalSchedGroupInfo getNextExecutableGroup() {
    while (true) {
      try {
        final GlobalSchedGroupInfo gi = activeQueue.take();
        return gi;
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Reschedule if the group is not inactive.
   */
  @Override
  public void reschedule(final GlobalSchedGroupInfo groupInfo, final boolean miss) {
    final boolean inactive = inactiveGroupChecker.check(groupInfo, miss);
    if (inactive) {
      groupStatusMap.get(groupInfo).set(false);
    } else {
      addGroup(groupInfo);
    }
  }

  @Override
  public void reschedule(final Collection<GlobalSchedGroupInfo> groupInfos) {
    for (final GlobalSchedGroupInfo groupInfo : groupInfos) {
      addGroup(groupInfo);
    }
  }

  @Override
  public void onNext(final GroupEvent groupEvent) {
    switch (groupEvent.getGroupEventType()) {
      case ADDITION:
        addGroup(groupEvent.getGroupInfo());
        break;
      case DELETION:
        removeGroup(groupEvent.getGroupInfo());
        break;
      default:
        throw new RuntimeException("Invalid group event type: " + groupEvent.getGroupEventType());
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      pollingService.shutdownNow();
    }
  }
}