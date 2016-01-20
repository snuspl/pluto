/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.task;

import edu.snu.mist.task.operator.Operator;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

// TODO[MIST-68]: Receive and deserialize logical plans into physical plans.
final class DefaultLogicalPlanReceiverImpl implements LogicalPlanReceiver {

  private EventHandler<PhysicalPlan<Operator>> eventHandler;

  @Inject
  private DefaultLogicalPlanReceiverImpl() {

  }

  @Override
  public void setHandler(final EventHandler<PhysicalPlan<Operator>> handler) {
    eventHandler = handler;
  }
}
