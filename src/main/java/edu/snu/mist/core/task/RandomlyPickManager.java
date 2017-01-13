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
package edu.snu.mist.core.task;

import javax.inject.Inject;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This class picks a head operator randomly.
 * It uses Random class for picking up an operator randomly.
 */
public final class RandomlyPickManager implements HeadOperatorManager {

  private final List<PhysicalOperator> queues;
  private final Random random;

  @Inject
  private RandomlyPickManager() {
    // [MIST-#]: For concurrency, it uses CopyOnWriteArrayList.
    // This could be a performance bottleneck.
    this.queues = new CopyOnWriteArrayList<>();
    this.random = new Random(System.currentTimeMillis());
  }

  @Override
  public void insert(final PhysicalOperator operator) {
    assert operator.isHeadOperator();
    queues.add(operator);
  }

  @Override
  public void delete(final PhysicalOperator operator) {
    queues.remove(operator);
  }

  @Override
  public PhysicalOperator pickHeadOperator() {
    while (true) {
      try {
        final int pick = random.nextInt(queues.size());
        final PhysicalOperator operator = queues.get(pick);
        return operator;
      } catch (final IllegalArgumentException e) {
        // This can occur when the size of queues is 0.
        // Return null.
        return null;
      } catch (final IndexOutOfBoundsException e) {
        return null;
      }
    }
  }
}
