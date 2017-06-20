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
package edu.snu.mist.core.task.globalsched.dispatch;

import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

/**
 * This assigns a group to a next group selector by mod operation.
 */
public final class ModGroupAssigner implements GroupAssigner {

  /**
   * Map that has group info as key and the index number as a value.
   */
  private final Map<GlobalSchedGroupInfo, Integer> groupIndexMap;

  /**
   * Next group selectors.
   */
  private final List<NextGroupSelector> selectors;

  /**
   * Stamped lock of next group selectors.
   */
  private final StampedLock selectorStampedLock;

  /**
   * Available group indexes.
   */
  private final Queue<Integer> availableIndices;

  /**
   * Group count.
   */
  private AtomicInteger count = new AtomicInteger(0);

  @Inject
  private ModGroupAssigner(final MistPubSubEventHandler mistPubSubEventHandler) {
    this.groupIndexMap = new ConcurrentHashMap<>();
    this.selectors = new LinkedList<>();
    this.selectorStampedLock = new StampedLock();
    this.availableIndices = new ConcurrentLinkedQueue<>();
    mistPubSubEventHandler.getPubSubEventHandler().subscribe(GroupEvent.class, this);
  }

  @Override
  public void assign(final GlobalSchedGroupInfo groupInfo) {
    final int index = groupIndexMap.get(groupInfo);

    long stamp = selectorStampedLock.tryOptimisticRead();

    // Modular operation to assign a selector
    NextGroupSelector selector = selectors.get(index % selectors.size());
    if (!selectorStampedLock.validate(stamp)) {
      stamp = selectorStampedLock.readLock();
      selector = selectors.get(index % selectors.size());
      selectorStampedLock.unlockRead(stamp);
    }

    selector.reschedule(groupInfo, false);
  }

  @Override
  public void addGroupSelector(final NextGroupSelector groupSelector) {
    final long stamp = selectorStampedLock.writeLock();
    selectors.add(groupSelector);
    selectorStampedLock.unlockWrite(stamp);
  }

  @Override
  public void removeGroupSelector(final NextGroupSelector groupSelector) {
    final long stamp = selectorStampedLock.writeLock();
    selectors.remove(groupSelector);
    selectorStampedLock.unlockWrite(stamp);
  }

  @Override
  public void onNext(final GroupEvent groupEvent) {
    switch (groupEvent.getGroupEventType()) {
      case ADDITION: {
        int index;
        if (availableIndices.size() != 0) {
          index = availableIndices.poll();
        } else {
          index = count.getAndIncrement();
        }
        groupIndexMap.put(groupEvent.getGroupInfo(), index);
        break;
      }
      case DELETION: {
        final int deletedIndex = groupIndexMap.remove(groupEvent.getGroupInfo());
        availableIndices.add(deletedIndex);
        break;
      }
      default:
        throw new RuntimeException("Invalid group event type: " + groupEvent.getGroupEventType());
    }
  }
}