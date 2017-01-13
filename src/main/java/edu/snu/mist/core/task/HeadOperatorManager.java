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

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface manages head operators.
 */
@DefaultImplementation(RandomlyPickManager.class)
public interface HeadOperatorManager {

  /**
   * Insert a head operator.
   * @param operator a head operator
   */
  void insert(PhysicalOperator operator);

  /**
   * Delete a head operator.
   * @param operator a head operator
   */
  void delete(PhysicalOperator operator);

  /**
   * Pick a head operator.
   * @return a head operator.
   * Returns null if there is no head operator.
   */
  PhysicalOperator pickHeadOperator();
}
